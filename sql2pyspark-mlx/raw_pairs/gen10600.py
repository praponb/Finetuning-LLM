from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

result = (
    lines
    .groupBy(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("row_count").desc_nulls_last())
)
