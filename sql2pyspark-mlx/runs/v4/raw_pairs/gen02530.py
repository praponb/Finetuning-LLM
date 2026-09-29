from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy("device")
    .agg(
        F.countDistinct("browser").alias("distinct_browsers"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_rows"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.count(F.when(F.col("browser") == "Edge", 1)).alias("edge_rows"),
    )
)
