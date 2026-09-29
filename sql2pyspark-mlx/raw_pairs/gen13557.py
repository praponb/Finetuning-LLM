from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy(F.col("budget_per_budget_adjusted").asc_nulls_last())
w2 = Window.partitionBy("location").orderBy("budget_per_budget_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("dept_name", "location").orderBy("budget_adjusted", "id")
w4 = Window.partitionBy("dept_name").orderBy("budget_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("dept_name").orderBy("budget", "id")

cleaned = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (~F.col("dept_name").eqNullSafe(F.lit("Legal"))))
    .select(
        "*",
        F.round((F.col("budget") * 1.1), 2).alias("budget_adjusted"),
        F.coalesce("location", F.lit("OTHER")).alias("location_clean"),
    )
)

base = (
    cleaned
    .filter(F.col("budget") <= 2960000)
    .select(
        "*",
        (F.col("budget") / F.nullif("budget_adjusted", F.lit(0))).alias("budget_per_budget_adjusted"),
    )
)

metrics = (
    base
    .select(
        "id",
        "dept_name",
        "budget",
        "budget_adjusted",
        "location_clean",
        "budget_per_budget_adjusted",
        F.rank().over(w_part).alias("budget_per_budget_adjusted_rank"),
        F.sum("budget_per_budget_adjusted").over(w2).alias("running_budget_per_budget_adjusted"),
        F.ntile(5).over(w_recent).alias("budget_adjusted_bucket"),
        F.last("location", ignorenulls=True).over(w4).alias("last_known_location"),
        F.lag("budget_adjusted", 2).over(w5).alias("prev_budget_adjusted"),
    )
)

kept = (
    metrics
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.salary").between(83000, 100000)), "left_anti")
)

result = (
    kept
    .groupBy(
        F.when(F.col("budget_adjusted_bucket") >= 8, "HIGH").otherwise("LOW").alias("budget_adjusted_bucket_tier"),
        F.when(F.col("budget_per_budget_adjusted") >= 69, "HIGH").otherwise("LOW").alias("budget_per_budget_adjusted_tier"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Sales", F.col("budget_adjusted"))).alias("sales_total_budget_adjusted"),
        F.count("*").alias("row_count"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
    )
    .orderBy(F.col("budget_adjusted_bucket_tier").desc())
)
