from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("location").orderBy(F.col("budget").asc_nulls_first())
w_ordered = Window.partitionBy("location").orderBy("adjusted_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_name").orderBy("adjusted_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("adjusted_budget", "id")
w_part = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("dept_name").orderBy("first_known_budget", "id")
w7 = Window.partitionBy("location", "dept_name")
w8 = Window.partitionBy("location").orderBy(F.col("first_known_budget").desc(), "id")

base = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (F.col("budget") <= 4010000))
    .select(
        "*",
        F.round((F.col("budget") * 0.85), 2).alias("adjusted_budget"),
        F.coalesce("dept_name", F.lit("UNKNOWN")).alias("dept_name_filled"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.dense_rank().over(w_running).alias("budget_dense_rank"),
        F.sum("adjusted_budget").over(w_ordered).alias("cumulative_adjusted_budget"),
        F.first("budget", ignorenulls=True).over(w3).alias("first_known_budget"),
        F.ntile(3).over(w4).alias("adjusted_budget_bucket"),
        F.sum("adjusted_budget").over(w_part).alias("cumulative_adjusted_budget_2"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.hire_date") >= "2024-11-10"), "left_anti")
)

metrics_departments = (
    eligible
    .withColumn("last_known_location", F.last("location", ignorenulls=True).over(w3))
    .withColumn("first_known_budget_bucket", F.ntile(4).over(w_hist))
    .withColumn("location_count_cumulative_adjusted_budget", F.count("cumulative_adjusted_budget").over(w7))
)

ranked = (
    metrics_departments
    .withColumn("dedup_rank", F.row_number().over(w8))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "dept_name",
        "budget",
        "adjusted_budget",
        "dept_name_filled",
        "budget_dense_rank",
        "first_known_budget",
        "adjusted_budget_bucket",
        "cumulative_adjusted_budget_2",
        F.when(F.col("dept_name").rlike("^(Marketing|HR)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("dept_name_zone"),
    )
    .orderBy("id")
    .limit(5)
)
