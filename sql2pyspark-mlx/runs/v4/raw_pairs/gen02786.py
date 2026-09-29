from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

with_last = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level").alias("employee_level"), F.col("c.job_title"))
)

result = (
    with_last
    .select(
        "id",
        "dept_name",
        "location",
        "employee_level",
        F.when(F.col("dept_name").rlike("^(Sales|Marketing)$"), "CORE").when(F.col("dept_name").rlike("^(Engineering|HR)$"), "PRIORITY").otherwise("GROUP_A").alias("dept_name_zone"),
    )
    .orderBy(F.col("dept_name").desc(), "dept_name_zone")
)
