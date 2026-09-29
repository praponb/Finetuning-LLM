from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget_times_budget_whole").desc(), "id")
w2 = Window.partitionBy("location").orderBy(F.col("adjusted_budget").desc(), "id")
w3 = Window.orderBy("budget_times_budget_whole", "id")
w4 = Window.orderBy("adjusted_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("location").orderBy("budget_times_budget_whole_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("location").orderBy("prev_budget_times_budget_whole", "id")
w7 = Window.orderBy("budget", "id")

valid = (
    df_departments
    .filter(F.col("location").isNotNull() & F.col("dept_name").isin(["Sales", "Legal", "Engineering"]))
    .withColumn("clean_location", F.trim("location"))
)

kept = (
    valid
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.level") == "L1"), "left_semi")
)

filtered = (
    kept
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.coalesce("location", F.lit("N/A")).alias("location_filled"),
        F.floor("budget").alias("budget_whole"),
        F.coalesce("dept_name", F.lit("OTHER")).alias("dept_name_filled"),
    )
)

filtered_departments = (
    filtered
    .filter(F.col("budget") >= 2075000)
    .select(
        "id",
        "location",
        "budget",
        "clean_location",
        "location_filled",
        "budget_whole",
        "dept_name_filled",
        F.round((F.col("budget") * 0.85), 2).alias("adjusted_budget"),
        (F.col("budget") * F.col("budget_whole")).alias("budget_times_budget_whole"),
        F.coalesce("location", F.lit("OTHER")).alias("resolved_location"),
    )
)

with_windows = (
    filtered_departments
    .withColumn("budget_times_budget_whole_bucket", F.ntile(10).over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("prev_budget_times_budget_whole", F.lag("budget_times_budget_whole", 1).over(w3))
)

enriched = (
    with_windows
    .withColumn("cumulative_budget_times_budget_whole_bucket", F.sum("budget_times_budget_whole_bucket").over(w4))
    .withColumn("first_known_budget_times_budget_whole", F.first("budget_times_budget_whole", ignorenulls=True).over(w5))
)

result = (
    enriched
    .withColumn("prev_cumulative_budget_times_budget_whole_bucket", F.lag("cumulative_budget_times_budget_whole_bucket", 1).over(w_grp))
    .withColumn("seq_no", F.row_number().over(w7))
)
