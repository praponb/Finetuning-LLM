from pyspark.sql import functions as F

flattened = df_carts.select("id", "created_at", F.explode_outer("coupon_codes").alias("tag"))

result = (
    flattened
    .groupBy(F.date_trunc("DAY", "created_at").alias("activity_day"), "tag")
    .agg(
        F.count("*").alias("cnt"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("tag") == "VIP", 1)).alias("vip_count"),
        F.count(F.when(F.col("tag") == "SPRING10", 1)).alias("spring10_rows"),
        F.min("created_at").alias("first_seen"),
        F.max("created_at").alias("last_seen"),
    )
    .orderBy(F.col("spring10_rows").asc_nulls_first())
)
