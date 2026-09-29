from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
    )
)

result = (
    lines
    .groupBy(F.date_trunc("DAY", "created_at").alias("activity_day"))
    .agg(
        F.percentile_approx("product_id", [0.25, 0.5, 0.75]).alias("product_id_quartiles"),
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
        F.percentile_approx("product_id", 0.9).alias("product_id_p90"),
    )
    .filter(F.col("product_id_p90") > 0)
)
