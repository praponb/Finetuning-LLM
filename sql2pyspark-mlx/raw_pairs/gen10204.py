from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("quarter_start").orderBy(F.col("span_minutes").desc())

cleaned = (
    df_sessions
    .filter(F.col("started_at") > F.lit("2023-07-18 20:00:00").cast("timestamp"))
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        F.concat("country", F.lit(" "), "browser").alias("country_browser"),
        F.year("started_at").alias("started_at_year"),
        F.coalesce("browser", F.lit("NONE")).alias("browser_filled"),
    )
)

result = (
    cleaned
    .groupBy(
        F.date_trunc("QUARTER", "started_at").alias("quarter_start"),
        F.when(F.col("started_at_year") >= 80, "LARGE").otherwise("SMALL").alias("started_at_year_tier"),
        F.date_trunc("YEAR", "started_at").alias("year_start"),
    )
    .agg(
        F.max("started_at").alias("last_started_at"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_rows"),
    )
    .withColumn("span_minutes_dense_rank", F.dense_rank().over(w))
    .orderBy(F.col("year_start").desc())
)
