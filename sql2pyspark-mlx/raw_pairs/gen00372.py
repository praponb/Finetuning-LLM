from pyspark.sql import functions as F

valid = (
    df_sessions
    .filter((F.col("started_at") >= F.to_date(F.lit("2023-03-07"))) & ~F.col("browser").isin(["Edge", "Safari", "Firefox"]))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.col("started_at").cast("date").alias("started_at_day"),
    )
)

result = (
    valid
    .groupBy(F.date_trunc("QUARTER", "started_at_day").alias("quarter_start"))
    .agg(
        F.count_if(F.col("device") == "mobile").alias("mobile_events"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
    )
)
