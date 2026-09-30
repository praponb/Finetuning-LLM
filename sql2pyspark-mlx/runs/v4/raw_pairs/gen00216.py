from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_name", "location")
w4 = Window.partitionBy("location", "dept_name").orderBy("running_budget", "id")

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select("*", F.trim("dept_name").alias("clean_dept_name"))
)

with_windows = (
    filtered
    .select(
        "*",
        F.lag("budget", 1).over(w).alias("prev_budget"),
        F.sum("budget").over(w2).alias("running_budget"),
    )
)

scored = (
    with_windows
    .withColumn("dept_name_max_running_budget", F.max("running_budget").over(w3))
    .withColumn("running_budget_bucket", F.ntile(10).over(w4))
)

cleaned = (
    scored
    .filter(F.col("dept_name_max_running_budget") >= 8712701)
    .select(
        "id",
        "location",
        "budget",
        "clean_dept_name",
        "prev_budget",
        "running_budget",
        "dept_name_max_running_budget",
        "running_budget_bucket",
        F.trim("location").alias("clean_location"),
        F.floor("prev_budget").alias("prev_budget_whole"),
        F.coalesce("dept_name", F.lit("NONE")).alias("resolved_dept_name"),
    )
)

filtered_departments = (
    cleaned
    .filter(F.col("location").isNotNull())
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
    .withColumn("location_filled", F.coalesce("location", F.lit("NONE")))
)

result = (
    filtered_departments
    .select(
        "id",
        "location",
        "clean_dept_name",
        "prev_budget",
        "dept_name_max_running_budget",
        "clean_location",
        "prev_budget_whole",
        "resolved_dept_name",
        "budget_filled",
    )
)
