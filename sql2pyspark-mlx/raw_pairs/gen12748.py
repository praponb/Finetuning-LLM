from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "value"),
    )
)

enriched = (
    exploded
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id").eqNullSafe(F.col("d.id")), "inner")
    .filter(~F.col("d.segment").isin(["Consumer", "Corporate"]))
    .select("a.id", "a.customer_id", "a.value", F.col("d.age"))
)

base = (
    enriched
    .filter(F.col("age") <= 39)
    .withColumn("age_rounded", F.round("age", 1))
    .withColumn("age_scaled", F.round((F.col("age") * 1.15), 2))
)

metrics = (
    base
    .select("*", F.round((F.col("age") / F.sum("age").over(w)), 4).alias("age_share"))
)

joined = (
    metrics
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select(
        "e.customer_id",
        "e.age_rounded",
        F.col("b.country"),
        F.col("b.signup_date"),
        F.col("b.name"),
    )
)

remaining_keys = (
    joined
    .select("customer_id")
    .exceptAll(
        df_carts
        .filter(F.col("customer_id").isNotNull())
        .select("customer_id")
    )
)

kept_rows = (
    joined
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

enriched_carts = (
    kept_rows
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.*", F.col("c.segment"), F.col("c.email"))
)

result = (
    enriched_carts
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .cube(F.col("d.city").alias("city"), F.col("s.segment").alias("cart_segment"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("s.age_rounded")).alias("max_age_rounded"),
        F.grouping_id().alias("grouping_level"),
    )
)
