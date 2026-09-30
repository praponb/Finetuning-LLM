from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy(F.date_trunc("QUARTER", "started_at").alias("quarter_start"))
    .agg(
        F.count(F.when(F.col("browser") == "Firefox", 1)).alias("firefox_rows"),
        F.min("started_at").alias("started_at"),
    )
    .orderBy(F.col("firefox_rows").desc_nulls_last())
)
