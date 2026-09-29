from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("element"),
    )
)

joined = (
    flattened
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.country"), F.col("p.name"), F.col("p.email"))
)

base = (
    joined
    .filter(F.col("element").isNotNull())
    .withColumn("element_filled", F.coalesce("element", F.lit("Unknown")))
)

filtered = (
    base
    .filter(F.col("country").isNotNull() & (F.col("created_at") >= F.lit("2025-03-01 18:00:00").cast("timestamp")))
    .select(
        "customer_id",
        "created_at",
        "element",
        "country",
        "name",
        "email",
        "element_filled",
        F.trim("name").alias("clean_name"),
    )
)

result = (
    filtered
    .select("customer_id", "element", "name", "email", "element_filled")
    .orderBy("element")
)
