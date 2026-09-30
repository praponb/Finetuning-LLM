from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("job_title").orderBy("hire_date", "id")

ranked = (
    df_employees
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_dims = (
    ranked
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.location"))
)

enriched = (
    with_dims
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "location",
        F.lag("salary", 2, 0).over(w2).alias("prev_salary"),
        F.ntile(5).over(w).alias("salary_bucket"),
    )
)

with_dims_employees = (
    enriched
    .alias("src")
    .join(df_departments.alias("p"), F.col("src.dept_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.location").alias("department_location"))
)

with_dims_employees_2 = (
    with_dims_employees
    .alias("e")
    .join(df_employees.alias("p"), F.col("e.manager_id") == F.col("p.id"), "inner")
    .filter(F.col("p.dept_id").isin([19, 39]))
    .select(
        "e.id",
        "e.dept_id",
        "e.manager_id",
        "e.salary",
        "e.location",
        "e.salary_bucket",
        F.col("p.salary").alias("employee_salary"),
        F.col("p.job_title").alias("employee_job_title"),
        F.col("p.email"),
    )
)

result = (
    with_dims_employees_2
    .select(
        "id",
        "dept_id",
        "salary",
        "location",
        "employee_salary",
        "employee_job_title",
        F.when(F.col("location").rlike("^(Bangkok|Berlin)$"), "CORE").when(F.col("location").rlike("^(Chiang Mai|New York)$"), "PRIORITY").otherwise("GROUP_B").alias("location_zone"),
    )
    .orderBy(F.col("location_zone").desc_nulls_first())
)
