from pyspark.sql import functions as F

lines = df_carts.select("id", "created_at", F.explode_outer("coupon_codes").alias("tag"))

result = (
    lines
    .groupBy(F.date_trunc("DAY", "created_at").alias("activity_day"))
    .agg(
        F.max("created_at").alias("last_seen"),
        F.max("created_at").alias("last_created_at"),
    )
)
