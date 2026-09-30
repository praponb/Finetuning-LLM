from pyspark.sql import functions as F

result = (
    df_sessions
    .withColumn("week_start", F.date_trunc("WEEK", "started_at"))
    .groupingSets([["week_start", "device"], ["week_start"]], "week_start", "device")
    .agg(
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.count_if(F.col("country") == "TH").alias("th_events"),
    )
)
