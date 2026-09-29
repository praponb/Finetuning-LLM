from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (~F.col("location").eqNullSafe(F.lit("Singapore"))))
    .withColumn("location_title", F.initcap(F.trim("location")))
)

result = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.location_title",
        F.col("c.job_title").alias("employee_job_title"),
        F.col("c.salary").alias("employee_salary"),
    )
)
