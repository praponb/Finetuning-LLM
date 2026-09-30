from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w_hist = Window.partitionBy("location", "dept_name").orderBy("running_max_budget", "id")
w_grp = Window.orderBy(F.col("running_min_budget").asc_nulls_last())

metrics = (
    df_departments
    .filter(F.col("location").isin(["Bangkok", "Singapore", "Chiang Mai"]))
    .withColumn("running_sum_budget", F.sum("budget").over(w))
    .withColumn("running_max_budget", F.max("budget").over(w))
    .withColumn("running_min_budget", F.min("budget").over(w))
    .withColumn("running_avg_budget", F.avg("budget").over(w))
    .withColumn("budget_bucket", F.ntile(5).over(w2))
)

enriched = (
    metrics
    .withColumn("prev_running_sum_budget", F.lag("running_sum_budget").over(w_hist))
    .withColumn("next_budget", F.lead("budget").over(w_hist))
    .withColumn("prev_budget", F.lag("budget").over(w_hist))
    .withColumn("seq_no", F.row_number().over(w_hist))
    .withColumn("running_min_budget_cume_dist", F.cume_dist().over(w_grp))
)

cleaned = (
    enriched
    .filter(F.col("dept_name").isNotNull())
    .select(
        "*",
        F.round((F.col("budget_bucket") * 1.1), 2).alias("budget_bucket_adjusted"),
    )
)

ranked = (
    cleaned
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "running_sum_budget",
        "budget_bucket",
        "next_budget",
        "prev_budget",
        "seq_no",
        "running_min_budget_cume_dist",
    )
)

result = (
    ranked
    .groupBy(
        F.when(F.col("budget") >= 4239000, "LARGE").otherwise("SMALL").alias("budget_tier"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "London", F.col("budget_bucket")).otherwise(0)).alias("london_budget_bucket"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.count(F.when(F.col("location") == "New York", 1)).alias("new_york_rows"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
    .filter(F.col("new_york_rows") > 1)
)
