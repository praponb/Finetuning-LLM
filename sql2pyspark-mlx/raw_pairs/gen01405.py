from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("hour_window").orderBy(F.col("duration_min").desc())

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "ship_to",
        "attributes",
        F.forall("items", lambda li: li["qty"] <= 5).alias("all_small_qty"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.coalesce(F.col("ship_to.country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
        F.coalesce(F.col("attributes").getItem("utm_source"), F.lit("none")).alias("utm_source_or_default"),
    )
)

result = (
    unpacked
    .groupBy(
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
        "customer_id",
        F.date_trunc("DAY", "created_at").alias("activity_day"),
    )
    .agg(
        F.min("created_at").alias("first_seen"),
        F.min("created_at").alias("first_created_at"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.min("created_at").alias("first_seen_2"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.max("created_at").alias("ended_at"),
    )
    .filter(F.col("span_minutes") < 1000000)
    .withColumn("duration_min_percent_rank", F.percent_rank().over(w))
    .orderBy("span_minutes")
)
