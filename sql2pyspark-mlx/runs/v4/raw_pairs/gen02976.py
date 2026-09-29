from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("tag"))
)

result = (
    lines
    .groupBy("tag", F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.max("created_at").alias("ended_at"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.count_if(F.col("tag") == "BUNDLE").alias("bundle_events"),
        F.countDistinct("tag").alias("distinct_tags"),
    )
    .filter(F.col("bundle_events") > 1)
)
