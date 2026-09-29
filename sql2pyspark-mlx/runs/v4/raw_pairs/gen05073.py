from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tag").orderBy("created_at_month")
w2 = Window.partitionBy("tag").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("tag")

flattened = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("tag"))

cleaned = (
    flattened
    .filter(F.col("tag").isin(["VIP", "BUNDLE"]))
    .withColumn("clean_tag", F.trim("tag"))
)

filtered = (
    cleaned
    .filter(F.col("tag").isNotNull() & F.col("tag").isin(["SPRING10", "VIP", "WELCOME5"]))
    .withColumn("created_at_month", F.month("created_at"))
    .withColumn("days_since_created_at", F.datediff(F.current_date(), "created_at"))
)

with_windows = (
    filtered
    .withColumn("created_at_month_cume_dist", F.cume_dist().over(w))
    .withColumn("created_at_month_max_1h", F.max("created_at_month").over(w2))
    .withColumn("tag_stddev_samp_created_at_month", F.stddev_samp("created_at_month").over(w3))
)

base = (
    with_windows
    .filter(F.col("tag").isNotNull())
    .select(
        "created_at",
        "clean_tag",
        "created_at_month",
        "days_since_created_at",
        "created_at_month_cume_dist",
        "created_at_month_max_1h",
        "tag_stddev_samp_created_at_month",
        F.coalesce("created_at_month_max_1h", F.lit(0)).alias("created_at_month_max_1h_filled"),
    )
)

filtered_carts = (
    base
    .filter(F.col("created_at") > F.lit("2025-03-02").cast("date"))
    .withColumn("created_at_month_cume_dist_level", F.when(F.col("created_at_month_cume_dist") >= 0.84, "High").when(F.col("created_at_month_cume_dist") >= 0.12, "Medium").otherwise("Low"))
    .withColumn("created_at_month_cume_dist_scaled", F.round((F.col("created_at_month_cume_dist") * 0.85), 2))
)

result = (
    filtered_carts
    .rollup(F.date_trunc("hour", "created_at").alias("hour_window"))
    .agg(
        F.max("created_at").alias("last_created_at"),
        F.round(F.avg("created_at_month"), 2).alias("avg_created_at_month"),
        F.min("created_at").alias("first_seen"),
        F.percentile_approx("created_at_month_cume_dist_scaled", 0.9).alias("created_at_month_cume_dist_scaled_p90"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.max("created_at").alias("ended_at"),
    )
    .orderBy(F.col("created_at_month_cume_dist_scaled_p90").desc())
)
