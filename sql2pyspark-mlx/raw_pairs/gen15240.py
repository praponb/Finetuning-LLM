from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("quarter_start").orderBy(F.col("is_new_burst_per_us").desc())

with_gaps = (
    df_sessions
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupBy(
        F.date_trunc("quarter", "started_at").alias("quarter_start"),
        F.when(F.col("island_id") >= 8, "PREMIUM").otherwise("STANDARD").alias("island_id_tier"),
        F.date_trunc("year", "started_at").alias("year_start"),
    )
    .agg(
        F.sum(F.when(F.col("browser") == "Edge", F.col("is_new_burst")).otherwise(0)).alias("edge_is_new_burst"),
        F.round(F.sum("is_new_burst") / F.nullif(F.count(F.when(F.col("country") == "US", 1)), F.lit(0)), 2).alias("is_new_burst_per_us"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
    )
    .withColumn("is_new_burst_per_us_percent_rank", F.percent_rank().over(w3))
)
