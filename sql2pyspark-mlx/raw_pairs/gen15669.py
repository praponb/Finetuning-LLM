from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

cleaned = (
    df_departments
    .filter((F.col("location").eqNullSafe(F.lit("Chiang Mai"))) & ~F.col("dept_name").isin(["Legal", "Sales", "Marketing"]))
    .withColumn("budget_rounded", F.round("budget", 0))
    .withColumn("location_lower", F.lower("location"))
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("N/A")))
)

result = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.budget_rounded",
        "s.dept_name_filled",
        F.col("c.hire_date"),
        F.col("c.job_title").alias("employee_job_title"),
    )
)
