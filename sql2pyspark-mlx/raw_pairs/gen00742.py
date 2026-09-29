from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "employee_job_title").orderBy(F.col("employee_salary").desc(), "id")
w2 = Window.partitionBy("employee_job_title").orderBy(F.col("salary").desc())

cleaned = (
    df_employees
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-03-26"))) & (~F.col("level").eqNullSafe(F.lit("L3"))))
    .select(
        "*",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.coalesce("job_title", F.lit("NONE")).alias("resolved_job_title"),
    )
)

enriched = (
    cleaned
    .alias("s")
    .join(df_employees.alias("p"), F.col("s.manager_id") == F.col("p.id"), "inner")
    .select(
        "s.*",
        F.col("p.job_title").alias("employee_job_title"),
        F.col("p.email").alias("employee_email"),
        F.col("p.salary").alias("employee_salary"),
    )
)

latest = (
    enriched
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

latest_employees = (
    latest
    .withColumn("row_num_2", F.rank().over(w2))
    .filter(F.col("row_num_2") == 1)
    .drop("row_num_2")
)

result = (
    latest_employees
    .select(
        "id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        "employee_job_title",
        "employee_email",
    )
    .orderBy(F.col("hire_date").desc(), "job_title")
)
