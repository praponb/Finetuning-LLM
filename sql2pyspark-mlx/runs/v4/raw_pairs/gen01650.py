from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("location").orderBy("budget")
w_ordered = Window.orderBy("budget", "id")

with_windows = (
    df_departments
    .select(
        "*",
        F.sum("budget").over(w_recent).alias("cumulative_budget"),
        F.dense_rank().over(w2).alias("budget_dense_rank"),
        F.ntile(5).over(w_ordered).alias("budget_bucket"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("budget_dense_rank") >= 9, "LARGE").otherwise("SMALL").alias("budget_dense_rank_tier"),
        "location",
        F.when(F.col("cumulative_budget") >= 20400184, "LARGE").otherwise("SMALL").alias("cumulative_budget_tier"),
    )
    .agg(
        F.percentile_approx("budget_bucket", [0.25, 0.5, 0.75]).alias("budget_bucket_quartiles"),
        F.percentile_approx("budget_bucket", 0.9).alias("budget_bucket_p90"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.percentile_approx("budget_dense_rank", 0.5).alias("budget_dense_rank_p50"),
        F.percentile_approx("budget_dense_rank", 0.5).alias("budget_dense_rank_p50_2"),
        F.sum(F.when(F.col("dept_name") == "Sales", F.col("budget_dense_rank")).otherwise(0)).alias("sales_budget_dense_rank"),
    )
    .filter(F.col("budget_bucket_p90") < 0)
)
