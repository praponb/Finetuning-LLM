from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy(
        F.date_trunc("QUARTER", "started_at").alias("quarter_start"),
        F.date_trunc("YEAR", "started_at").alias("year_start"),
    )
    .agg(
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_rows"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.min("started_at").alias("started_at"),
    )
    .orderBy(F.col("sg_rows").desc())
)
