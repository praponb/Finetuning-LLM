from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id")
w2 = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc_nulls_last(), "id")

deduped = (
    df_employees
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
    )
)

latest = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

with_dims = (
    latest
    .alias("src")
    .join(df_employees.alias("p"), F.col("src.manager_id") == F.col("p.id"), "inner")
    .select(
        "src.id",
        "src.email",
        "src.manager_id",
        "src.salary",
        "src.hire_date",
        "src.is_remote",
        "src.level",
        "src.rank_in_group",
        F.col("p.full_name"),
        F.col("p.salary").alias("employee_salary"),
    )
)

cleaned = (
    with_dims
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-05-19"))) & F.col("manager_id").isin([10, 18, 20, 35]))
    .select(
        "id",
        "email",
        "manager_id",
        "salary",
        "hire_date",
        "level",
        "rank_in_group",
        "employee_salary",
        F.substring("level", 1, 4).alias("level_prefix"),
    )
)

with_dims_employees = (
    cleaned
    .alias("e")
    .join(df_employees.alias("d"), F.col("e.manager_id") == F.col("d.id"), "left")
    .select(
        "e.*",
        F.col("d.job_title"),
        F.col("d.salary").alias("employee_salary_2"),
        F.col("d.hire_date").alias("employee_hire_date"),
    )
)

result = (
    with_dims_employees
    .select(
        "id",
        "manager_id",
        F.col("salary").alias("SALARY_VAL"),
        "employee_salary_2",
        "employee_salary",
    )
    .unpivot(
        ids=["id", "manager_id"],
        values=["SALARY_VAL", "employee_salary_2", "employee_salary"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
