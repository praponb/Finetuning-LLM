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

with_dims = (
    flattened
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.length(F.col("c.email")) >= 13)
    .select("a.customer_id", "a.created_at", F.col("c.city"), F.col("c.age"))
)

base = (
    with_dims
    .filter((F.col("created_at") >= F.lit("2025-03-02 03:00:00").cast("timestamp")) & F.col("city").isNotNull())
    .withColumn("age_str", F.col("age").cast("double"))
)

cleaned = (
    base
    .filter((F.col("created_at") >= F.lit("2025-03-01 14:00:00").cast("timestamp")) & (F.col("customer_id") == 38))
    .select("*", F.date_trunc("WEEK", "created_at").alias("created_at_period"))
)

result = (
    cleaned
    .groupBy("customer_id")
    .pivot("city", ["New York", "Singapore"])
    .agg(F.avg("age_str").alias("amount"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "customer_id",
        F.col("New York_amount").alias("new_york_amount"),
        F.col("New York_item_count").alias("new_york_item_count"),
        F.col("Singapore_amount").alias("singapore_amount"),
        F.col("Singapore_item_count").alias("singapore_item_count"),
    )
)
