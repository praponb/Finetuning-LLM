from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").asc_nulls_last(), "id")
w2 = Window.orderBy(F.col("location_length").desc(), "id")
w3 = Window.partitionBy("dept_name", "location").orderBy("budget", "id")
w4 = Window.partitionBy("dept_name")
w5 = Window.orderBy("budget", "id")
w6 = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_last())

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select("*", F.coalesce("location", F.lit("Unknown")).alias("location_filled"))
)

base = (
    valid
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.length("location").alias("location_length"),
        F.when(F.col("location").isin(["Berlin", "New York"]), "Tier 1").otherwise("Other").alias("location_group"),
    )
)

deduped = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    deduped
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("budget_bucket", F.ntile(5).over(w3))
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w4)), 4))
    .withColumn("next_budget", F.lead("budget", 1).over(w5))
    .withColumn("budget_rank", F.rank().over(w6))
)

cleaned = (
    metrics
    .filter(F.col("location").isNotNull())
    .withColumn("event_seq_minus_budget_bucket", (F.col("event_seq") - F.col("budget_bucket")))
    .withColumn("budget_bucket_minus_dedup_rank", (F.col("budget_bucket") - F.col("dedup_rank")))
)

grouped = (
    cleaned
    .withColumn("budget_rank_tier", F.when(F.col("budget_rank") >= 8, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["budget_rank_tier", "location"], ["budget_rank_tier"], []],
        "budget_rank_tier",
        "location",
    )
    .agg(
        F.round(F.sum("budget_rank") / F.nullif(F.count(F.when(F.col("location") == "Tokyo", 1)), F.lit(0)), 2).alias("budget_rank_per_tokyo"),
        F.count(F.when(F.col("location") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.count_if(F.col("dept_name") == "Marketing").alias("marketing_events"),
        F.grouping("budget_rank_tier").alias("grouping_level"),
    )
)

result = (
    grouped
    .select("marketing_events", "grouping_level")
    .orderBy(F.col("grouping_level").desc(), "marketing_events")
)
