from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

with_dims = (
    flattened
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id").eqNullSafe(F.col("c.id")), "inner")
    .filter(F.col("c.city").isin(["Berlin", "Bangkok"]))
    .select(
        "src.id",
        "src.customer_id",
        F.col("c.city"),
        F.col("c.name"),
        F.col("c.age"),
    )
)

cleaned = (
    with_dims
    .filter(F.upper(F.trim("name")) == "CAROL DIAZ")
    .select(
        "id",
        "customer_id",
        F.coalesce("age", F.lit(0)).alias("age_filled"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("resolved_city"),
    )
)

base = (
    cleaned
    .filter(F.col("customer_id").isNotNull())
    .withColumn("age_filled_scaled", F.round((F.col("age_filled") * 0.9), 2))
    .withColumn("age_filled_band", F.when(F.col("age_filled") >= 75, "High").when(F.col("age_filled") >= 32, "Medium").otherwise("Low"))
)

enriched = (
    base
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select("e.id", "e.customer_id", "e.age_filled_scaled", F.col("c.age"))
)

joined = (
    enriched
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.segment"), F.col("d.email"), F.col("d.name"))
)

result = (
    joined
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "age_filled_scaled",
        "segment",
        "name",
        F.col("min_account_balance"),
    )
    .orderBy(F.col("segment").asc_nulls_first(), "age_filled_scaled")
)
