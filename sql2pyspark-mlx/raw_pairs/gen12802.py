from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_name")

enriched = (
    df_departments
    .select(
        "id",
        "location",
        F.count("budget").over(w).alias("running_count_budget"),
        F.min("budget").over(w).alias("running_min_budget"),
        F.sum("budget").over(w).alias("running_sum_budget"),
        F.avg("budget").over(w).alias("running_avg_budget"),
        F.stddev_samp("budget").over(w2).alias("dept_name_stddev_samp_budget"),
    )
)

result = (
    enriched
    .select(
        "id",
        "location",
        "running_count_budget",
        "running_avg_budget",
        "dept_name_stddev_samp_budget",
    )
    .orderBy(F.col("running_avg_budget").asc_nulls_first())
)
