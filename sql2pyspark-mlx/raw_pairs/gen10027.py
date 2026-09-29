from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "customer_id").orderBy(F.col("started_at").desc_nulls_last(), "id")

deduped = (
    df_sessions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    deduped
    .withColumn("quarter_start", F.date_trunc("QUARTER", "started_at"))
    .withColumn("year_start", F.date_trunc("YEAR", "started_at"))
    .withColumn("week_start", F.date_trunc("WEEK", "started_at"))
    .groupingSets(
        [["quarter_start", "year_start", "week_start"], ["quarter_start", "year_start"], ["quarter_start"]],
        "quarter_start",
        "year_start",
        "week_start",
    )
    .agg(
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_count"),
        F.countDistinct("browser").alias("distinct_browsers"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.min("started_at").alias("started_at"),
        F.min("started_at").alias("first_seen"),
    )
    .orderBy(F.col("distinct_browsers").desc())
)
