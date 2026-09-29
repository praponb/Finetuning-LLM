from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy(
        F.date_trunc("QUARTER", "started_at").alias("quarter_start"),
        F.date_trunc("WEEK", "started_at").alias("week_start"),
    )
    .agg(
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes_2"),
        F.countDistinct("country").alias("distinct_countries"),
        F.countDistinct("browser").alias("distinct_browsers"),
    )
    .orderBy(F.col("span_minutes").desc_nulls_last())
)
