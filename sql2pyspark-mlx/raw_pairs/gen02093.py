from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("budget", "id").rowsBetween(-2, -1)
w_running_2 = Window.orderBy(F.col("budget").desc_nulls_last())
w3 = Window.partitionBy("location", "dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_name", "location").orderBy(F.col("budget_dense_rank").desc(), "id")
w5 = Window.partitionBy("location").orderBy(F.col("adjusted_budget_avg_2_prior").asc_nulls_first())
w6 = Window.partitionBy("location", "dept_name").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("location")
w8 = Window.partitionBy("dept_name")
w9 = Window.partitionBy("location").orderBy(F.col("adjusted_budget_avg_2_prior_cume_dist").asc_nulls_last())
w10 = Window.partitionBy("dept_name", "location").orderBy(F.col("adjusted_budget_avg_2_prior_cume_dist").desc(), "id")
w11 = Window.partitionBy("cumulative_adjusted_budget_avg_2_prior_tier").orderBy(F.col("num_dept_names").desc())

cleaned = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("budget_level", F.when(F.col("budget") >= 3211000, "High").when(F.col("budget") >= 1624000, "Medium").otherwise("Low"))
    .withColumn("dept_name_clean", F.coalesce("dept_name", F.lit("DIRECT")))
)

with_windows = (
    cleaned
    .filter(F.col("dept_name").isin(["Sales", "HR"]))
    .select(
        "*",
        F.avg("budget").over(w_running).alias("budget_avg_2_prior"),
        F.dense_rank().over(w_running_2).alias("budget_dense_rank"),
        F.sum("budget").over(w3).alias("running_budget"),
    )
)

latest = (
    with_windows
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

cleaned_departments = (
    latest
    .filter(F.col("dept_name").isNotNull())
    .select(
        "*",
        F.round((F.col("budget_avg_2_prior") * 0.85), 2).alias("adjusted_budget_avg_2_prior"),
        F.coalesce("location", F.lit("OTHER")).alias("location_clean"),
    )
)

with_windows_departments = (
    cleaned_departments
    .select(
        "*",
        F.cume_dist().over(w5).alias("adjusted_budget_avg_2_prior_cume_dist"),
        F.sum("adjusted_budget_avg_2_prior").over(w6).alias("cumulative_adjusted_budget_avg_2_prior"),
        F.round((F.col("budget_dense_rank") / F.sum("budget_dense_rank").over(w7)), 4).alias("budget_dense_rank_share"),
    )
)

metrics = (
    with_windows_departments
    .select(
        "*",
        F.round((F.col("budget_avg_2_prior") / F.sum("budget_avg_2_prior").over(w8)), 4).alias("budget_avg_2_prior_share"),
        F.dense_rank().over(w9).alias("adjusted_budget_avg_2_prior_cume_dist_dense_rank"),
        F.row_number().over(w10).alias("event_seq"),
        F.min("budget_dense_rank_share").over(w8).alias("dept_name_min_budget_dense_rank_share"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("cumulative_adjusted_budget_avg_2_prior") >= 400, "HIGH").otherwise("LOW").alias("cumulative_adjusted_budget_avg_2_prior_tier"),
        "location",
    )
    .agg(
        F.size(F.collect_set("location")).alias("num_locations"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
    )
    .filter(F.col("num_locations") > 1)
    .withColumn("num_dept_names_rank", F.rank().over(w11))
)
