from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy(F.date_trunc("MONTH", "started_at").alias("activity_month"))
    .agg(
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.count(F.when(F.col("browser") == "Firefox", 1)).alias("firefox_count"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
    )
    .filter(F.col("uk_count") > 1)
)
