from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("budget").desc(), "id")
w2 = Window.orderBy("budget", "id")
w_recent = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_last())
w4 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("location", "dept_name").orderBy("budget", "id").rowsBetween(-3, -1)

metrics = (
    df_departments
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.row_number().over(w).alias("row_seq"),
        F.lag("budget", 1).over(w2).alias("prev_budget"),
        F.rank().over(w_recent).alias("budget_rank"),
        F.sum("budget").over(w4).alias("running_budget"),
        F.avg("budget").over(w5).alias("budget_avg_3_prior"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("budget") >= 3740000, "LARGE").otherwise("SMALL").alias("budget_tier"),
    )
    .agg(
        F.countDistinct("location").alias("distinct_locations"),
        F.size(F.collect_set("location")).alias("num_locations"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
    )
)
