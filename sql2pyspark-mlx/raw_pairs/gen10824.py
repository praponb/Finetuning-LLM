from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

result = (
    lines
    .groupBy("customer_id", F.date_trunc("day", "created_at").alias("activity_day"))
    .agg(
        F.min("created_at").alias("first_seen"),
        F.count(F.when(F.col("promo") == "WELCOME5", 1)).alias("welcome5_count"),
        F.sum("price").alias("total_price"),
        F.percentile_approx("price", 0.95).alias("price_p95"),
    )
    .orderBy(F.col("activity_day").asc_nulls_last())
)
