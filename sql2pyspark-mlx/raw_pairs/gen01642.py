from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("is_tokyo", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w3 = Window.partitionBy("location").orderBy(F.col("is_tokyo").desc(), "id")
w4 = Window.partitionBy("dept_name").orderBy("is_tokyo", "id")
w5 = Window.orderBy("is_tokyo", "id")
w6 = Window.orderBy("first_known_budget", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.orderBy("budget_bucket", "id")
w8 = Window.orderBy(F.col("event_seq").desc_nulls_last())
w9 = Window.orderBy("event_seq", "id").rowsBetween(-3, Window.currentRow)
w10 = Window.orderBy("next_is_tokyo", "id")
w11 = Window.orderBy("next_is_tokyo", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.concat_ws("/", "location", "dept_name").alias("location_dept_name_key"),
        F.when(F.col("location") == "Tokyo", 1).otherwise(0).alias("is_tokyo"),
        F.coalesce("dept_name", F.lit("OTHER")).alias("dept_name_filled"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "budget",
        "location_dept_name_key",
        F.first("budget", ignorenulls=True).over(w).alias("first_known_budget"),
        F.ntile(4).over(w2).alias("budget_bucket"),
        F.row_number().over(w3).alias("event_seq"),
        F.ntile(3).over(w4).alias("is_tokyo_bucket"),
        F.lead("is_tokyo", 1).over(w5).alias("next_is_tokyo"),
    )
)

cleaned = (
    metrics
    .filter(F.col("first_known_budget").between(3042000, 3328000))
    .select(
        "id",
        "budget",
        "first_known_budget",
        "budget_bucket",
        "event_seq",
        "next_is_tokyo",
        (F.col("is_tokyo_bucket") + F.col("budget_bucket")).alias("is_tokyo_bucket_plus_budget_bucket"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.avg("budget").over(w6).alias("budget_avg_3"),
        F.lead("next_is_tokyo", 1).over(w7).alias("next_next_is_tokyo"),
        F.cume_dist().over(w8).alias("event_seq_cume_dist"),
        F.avg("event_seq").over(w9).alias("event_seq_avg_3"),
        F.ntile(3).over(w10).alias("next_is_tokyo_bucket"),
    )
)

scored = (
    enriched
    .withColumn("running_min_first_known_budget", F.min("first_known_budget").over(w11))
    .withColumn("running_sum_first_known_budget", F.sum("first_known_budget").over(w11))
    .withColumn("running_max_first_known_budget", F.max("first_known_budget").over(w11))
    .withColumn("running_count_first_known_budget", F.count("first_known_budget").over(w11))
)

result = (
    scored
    .cube(
        F.when(F.col("event_seq") >= 7, "PREMIUM").otherwise("STANDARD").alias("event_seq_tier"),
        F.when(F.col("running_min_first_known_budget") >= 3317000, "HIGH").otherwise("LOW").alias("running_min_first_known_budget_tier"),
    )
    .agg(
        F.percentile_approx("next_is_tokyo", [0.25, 0.5, 0.75]).alias("next_is_tokyo_quartiles"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("num_rows").desc_nulls_first())
)
