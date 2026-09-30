from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("budget").desc(), "id")
w3 = Window.partitionBy("location").orderBy(F.col("running_count_budget").desc_nulls_last(), "id")
w4 = Window.partitionBy("location")
w5 = Window.partitionBy("location").orderBy("running_sum_budget")

enriched = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        F.count("budget").over(w).alias("running_count_budget"),
        F.sum("budget").over(w).alias("running_sum_budget"),
        F.min("budget").over(w).alias("running_min_budget"),
        F.ntile(4).over(w2).alias("budget_bucket"),
    )
)

latest = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

matched_keys = (
    latest
    .select("location", "id")
    .subtract(
        df_departments
        .filter(F.col("dept_name") != "Legal")
        .select("location", "id")
    )
)

verified = (
    latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.location") == F.col("k.location")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("budget_bucket").isNotNull())
    .withColumn("running_min_budget_times_budget_bucket", (F.col("running_min_budget") * F.col("budget_bucket")))
)

with_windows = (
    valid
    .select(
        "location",
        "running_sum_budget",
        "running_min_budget",
        "running_min_budget_times_budget_bucket",
        F.max("running_min_budget").over(w4).alias("location_max_running_min_budget"),
        F.cume_dist().over(w5).alias("running_sum_budget_cume_dist"),
        F.round((F.col("budget") / F.sum("budget").over(w4)), 4).alias("budget_share"),
    )
)

result = (
    with_windows
    .cube(
        "location",
        F.when(F.col("running_min_budget_times_budget_bucket") >= 79, "PREMIUM").otherwise("STANDARD").alias("running_min_budget_times_budget_bucket_tier"),
    )
    .agg(
        F.count(F.when(F.col("location") == "Bangkok", 1)).alias("bangkok_rows"),
        F.sum(F.when(F.col("location") == "Singapore", F.col("running_sum_budget_cume_dist"))).alias("singapore_total_running_sum_budget_cume_dist"),
    )
    .orderBy(F.col("singapore_total_running_sum_budget_cume_dist").asc_nulls_last())
)
