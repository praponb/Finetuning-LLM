from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("employee_salary").desc_nulls_first(), "id")
w2 = Window.partitionBy("manager_id", "job_title").orderBy(F.col("rank_in_group").desc(), "id")
w3 = Window.partitionBy("job_title").orderBy(F.col("salary_adjusted").desc(), "id")

enriched = (
    df_employees
    .alias("e")
    .join(df_employees.alias("d"), F.col("e.manager_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.full_name",
        "e.dept_id",
        "e.manager_id",
        "e.salary",
        F.col("d.salary").alias("employee_salary"),
    )
)

with_dims = (
    enriched
    .alias("src")
    .join(df_employees.alias("p"), F.col("src.manager_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "src.id",
        "src.full_name",
        "src.manager_id",
        "src.salary",
        "src.employee_salary",
        F.col("p.email"),
        F.col("p.full_name").alias("employee_full_name"),
        F.col("p.job_title"),
    )
)

latest = (
    with_dims
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

ranked = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "full_name",
        "manager_id",
        "salary",
        "employee_full_name",
        "job_title",
    )
)

cleaned = (
    ranked
    .filter(F.col("salary") > 120000)
    .select(
        "*",
        F.regexp_replace("full_name", " ", "-").alias("full_name_slug"),
        F.round((F.col("salary") * 1.15), 2).alias("salary_adjusted"),
    )
)

latest_employees = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest_employees
    .select(
        "id",
        "full_name",
        "manager_id",
        "salary",
        "employee_full_name",
        "job_title",
        "full_name_slug",
        "salary_adjusted",
        "latest_rank",
    )
    .orderBy("manager_id")
)
