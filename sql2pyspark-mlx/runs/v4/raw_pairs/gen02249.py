from pyspark.sql import functions as F

cleaned = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-10-07 20:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        F.date_trunc("year", "paid_at").alias("paid_at_period"),
    )
)

result = (
    cleaned
    .groupBy(F.date_trunc("week", "paid_at_period").alias("week_start"))
    .agg(
        ((F.unix_timestamp(F.max("paid_at_period")) - F.unix_timestamp(F.min("paid_at_period"))) / 60).alias("span_minutes"),
        F.count("*").alias("cnt"),
        F.size(F.collect_set("method")).alias("num_methods"),
        F.count_if(F.col("method") == "cash").alias("cash_events"),
        F.count_if(F.col("method") == "cash").alias("cash_events_2"),
    )
    .orderBy("cash_events_2")
)
