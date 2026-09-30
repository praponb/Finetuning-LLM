from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("attributes").alias("key", "value"),
    )
)

result = (
    lines
    .groupBy(
        "key",
        F.date_trunc("DAY", "created_at").alias("activity_day"),
        "customer_id",
    )
    .agg(
        F.count_if(F.col("key") == "gift").alias("gift_events"),
        F.count("*").alias("cnt"),
        F.count_if(F.col("key") == "utm_source").alias("utm_source_events"),
        F.count_if(F.col("key") == "gift").alias("gift_events_2"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.count("*").alias("cnt_2"),
    )
)
