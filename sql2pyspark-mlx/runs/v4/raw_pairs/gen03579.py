from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.orderBy("employee_hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")
w4 = Window.partitionBy("location", "dept_name").orderBy("employee_hire_date", "id")

kept = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.email").like("%@GMAIL.COM")), "left_anti")
)

with_last = (
    kept
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary"), F.col("c.hire_date").alias("employee_hire_date"))
)

with_windows = (
    with_last
    .withColumn("first_known_dept_name", F.first("dept_name", ignorenulls=True).over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("next_salary", F.lead("salary", 1).over(w4))
)

result = (
    with_windows
    .groupBy("location")
    .pivot("dept_name", ["Legal", "Marketing", "HR"])
    .agg(F.avg("next_salary"))
    .select(
        "location",
        F.col("Legal").alias("legal"),
        F.col("Marketing").alias("marketing"),
        F.col("HR").alias("hr"),
    )
)
