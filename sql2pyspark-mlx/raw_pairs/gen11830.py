from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget", "id")
w_ordered = Window.orderBy("budget", "id")
w3 = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_departments
    .filter(F.col("dept_name").isin(["Legal", "Finance", "HR"]))
    .select(
        "*",
        F.trim("dept_name").alias("clean_dept_name"),
        F.coalesce("dept_name", F.lit("DIRECT")).alias("resolved_dept_name"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (~F.col("ch.email").like("%@yahoo.com")), "left_anti")
)

valid = (
    not_flagged
    .filter(F.col("budget") <= 1252000)
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "clean_dept_name",
        "resolved_dept_name",
        F.concat("location", F.lit("/"), "dept_name").alias("location_dept_name"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.ntile(3).over(w).alias("budget_bucket"),
        F.lag("budget", 2).over(w_ordered).alias("prev_budget"),
        F.first("budget", ignorenulls=True).over(w3).alias("first_known_budget"),
    )
)

result = (
    metrics
    .groupBy("dept_name")
    .pivot("location", ["Chiang Mai", "Tokyo"])
    .agg(F.sum("prev_budget"))
    .select(
        "dept_name",
        F.col("Chiang Mai").alias("chiang_mai"),
        F.col("Tokyo").alias("tokyo"),
    )
)
