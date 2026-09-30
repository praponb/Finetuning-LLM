from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("started_at"), "customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country", "customer_id").orderBy(F.col("row_seq_whole").desc_nulls_last(), "id")

cleaned = (
    df_sessions
    .filter(F.col("country").isNotNull())
    .select("*", F.upper("device").alias("device_upper"))
)

enriched = (
    cleaned
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w))
    .withColumn("row_seq", F.row_number().over(w_part))
    .withColumn("first_known_country_2", F.first("country", ignorenulls=True).over(w3))
)

base = (
    enriched
    .filter(F.col("browser").isNotNull() & (F.col("started_at") > F.lit("2023-03-06 23:00:00").cast("timestamp")))
    .withColumn("row_seq_whole", F.floor("row_seq"))
    .withColumn("country_filled", F.coalesce("country", F.lit("N/A")))
)

deduped = (
    base
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .groupBy(
        "device",
        "browser",
        F.when(F.col("row_seq") >= 7, "HIGH").otherwise("LOW").alias("row_seq_tier"),
    )
    .agg(
        F.percentile_approx("row_seq_whole", [0.25, 0.5, 0.75]).alias("row_seq_whole_quartiles"),
        F.round(F.avg("row_seq_whole"), 2).alias("avg_row_seq_whole"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.round(F.sum("row_seq_whole") / F.nullif(F.count(F.when(F.col("browser") == "Firefox", 1)), F.lit(0)), 2).alias("row_seq_whole_per_firefox"),
        F.percentile_approx("row_seq", 0.95).alias("row_seq_p95"),
    )
    .filter(F.col("row_seq_whole_per_firefox") < 1000000)
    .orderBy(F.col("num_browsers").asc_nulls_last())
)
