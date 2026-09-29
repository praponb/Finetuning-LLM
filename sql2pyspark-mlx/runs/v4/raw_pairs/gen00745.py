from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("employee_level", "manager_id")
w2 = Window.partitionBy("dept_id", "employee_level").orderBy("adjusted_salary", "id")
w3 = Window.partitionBy("employee_level", "dept_id").orderBy(F.col("salary_share").desc())

cleaned = (
    df_employees
    .filter(F.col("manager_id") == 39)
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.round((F.col("salary") * 0.9), 2).alias("adjusted_salary"),
    )
)

with_dims = (
    cleaned
    .alias("e")
    .join(df_employees.alias("d"), F.col("e.manager_id") == F.col("d.id"), "inner")
    .select(
        "e.*",
        F.col("d.full_name").alias("employee_full_name"),
        F.col("d.level").alias("employee_level"),
        F.col("d.email").alias("employee_email"),
    )
)

enriched = (
    with_dims
    .select(
        "*",
        F.round((F.col("salary") / F.sum("salary").over(w)), 4).alias("salary_share"),
        F.lag("salary", 1).over(w2).alias("prev_salary"),
    )
)

latest = (
    enriched
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .select(
        "id",
        "full_name",
        "email",
        "salary",
        "is_remote",
        "level",
        "employee_full_name",
        "employee_level",
        "employee_email",
    )
    .orderBy(F.col("salary").asc_nulls_first())
)
