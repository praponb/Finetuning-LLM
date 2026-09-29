from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.orderBy("budget_times_budget_int", "id")
w3 = Window.partitionBy("location").orderBy("budget_int", "id")
w4 = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_departments
    .filter((F.col("budget") < 3086000) & (F.col("dept_name").eqNullSafe(F.lit("HR"))))
    .withColumn("budget_int", F.col("budget").cast("int"))
)

valid = (
    filtered
    .filter(F.col("dept_name").isin(["Engineering", "Sales", "Legal"]))
    .withColumn("budget_times_budget_int", (F.col("budget") * F.col("budget_int")))
)

not_flagged = (
    valid
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.salary") < 65000), "left_anti")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date").alias("employee_hire_date"))
)

scored = (
    latest_match
    .withColumn("prev_budget_int", F.lag("budget_int", 2).over(w2))
    .withColumn("budget_int_bucket", F.ntile(5).over(w3))
    .withColumn("running_budget_int", F.sum("budget_int").over(w4))
)

pivoted = (
    scored
    .groupBy("dept_name")
    .pivot("location", ["Berlin", "Singapore"])
    .agg(F.max("budget_times_budget_int"))
)

result = (
    pivoted
    .select("dept_name", "Berlin", "Singapore")
    .orderBy(F.col("Singapore").desc())
)
