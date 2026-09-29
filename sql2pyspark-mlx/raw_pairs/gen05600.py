from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy(F.year("started_at"), "device").orderBy(F.col("started_at").desc(), "id")

metrics = (
    df_sessions
    .filter(F.col("customer_id") == 13)
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w))
    .withColumn("row_seq", F.row_number().over(w_running))
)

result = (
    metrics
    .groupBy("browser")
    .agg(
        F.countDistinct("country").alias("distinct_countries"),
        F.round(F.sum("row_seq") / F.nullif(F.count(F.when(F.col("browser") == "Edge", 1)), F.lit(0)), 2).alias("row_seq_per_edge"),
        F.count("*").alias("event_count"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
    )
    .filter(F.col("event_count") >= 1)
    .orderBy(F.col("row_seq_per_edge").asc_nulls_last())
)
