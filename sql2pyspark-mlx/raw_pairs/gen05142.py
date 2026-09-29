from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary", "id")
w2 = Window.partitionBy("job_title", "dept_id").orderBy(F.col("hire_date").desc_nulls_last(), "id")
w3 = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc_nulls_last(), "id")
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.min("salary").alias("min_employee_salary"))

ranked = (
    df_employees
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
    )
)

ranked_employees = (
    ranked
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .select("id", "manager_id", "hire_date", "level")
)

ranked_employees_2 = (
    ranked_employees
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

joined = (
    ranked_employees_2
    .alias("a")
    .join(df_employees.alias("e"), F.col("a.manager_id").eqNullSafe(F.col("e.id")), "inner")
    .filter(F.col("e.dept_id").isin([5, 6, 39]))
    .select(
        "a.id",
        "a.manager_id",
        "a.level",
        "a.rn",
        F.col("e.full_name"),
        F.col("e.email"),
    )
)

enriched = (
    joined
    .alias("e")
    .join(df_employees.alias("p"), F.col("e.manager_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "e.id",
        "e.manager_id",
        "e.level",
        "e.full_name",
        "e.email",
        F.col("p.email").alias("employee_email"),
        F.col("p.job_title"),
    )
)

filtered = (
    enriched
    .filter(F.col("job_title").isNotNull())
    .withColumn("level_job_title", F.concat("level", F.lit(" "), "job_title"))
)

with_dims = (
    filtered
    .alias("a")
    .join(df_employees.alias("b"), F.col("a.manager_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.manager_id",
        "a.full_name",
        "a.email",
        "a.employee_email",
        F.col("b.email").alias("employee_email_2"),
    )
)

result = (
    with_dims
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "manager_id",
        "full_name",
        "email",
        "employee_email",
        "employee_email_2",
        F.col("min_employee_salary"),
    )
)
