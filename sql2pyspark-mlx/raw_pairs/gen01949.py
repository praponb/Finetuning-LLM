from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter((F.col("started_at") >= F.to_date(F.lit("2023-04-04"))) & F.col("customer_id").isNull())
    .withColumn("browser_group", F.when(F.col("browser").isin(["Firefox", "Edge"]), "Group A").otherwise("Other"))
    .withColumn("browser_clean", F.coalesce("browser", F.lit("NONE")))
)

with_gaps = (
    cleaned
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        "browser_clean",
        F.sum("is_new_session").over(w2).alias("session_no"),
    )
)

result = (
    islands
    .groupBy("country")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("browser") == "Chrome", 1)).alias("chrome_rows"),
        F.round(F.sum("session_no") / F.nullif(F.count(F.when(F.col("browser") == "Safari", 1)), F.lit(0)), 2).alias("session_no_per_safari"),
        F.countDistinct("country").alias("distinct_countries"),
        F.avg("session_no").alias("avg_session_no"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .orderBy(F.col("country").asc_nulls_first())
)
