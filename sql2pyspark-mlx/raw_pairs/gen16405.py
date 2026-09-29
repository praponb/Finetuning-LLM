from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("employee_salary").desc_nulls_last(), "id")
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.min("salary").alias("min_employee_salary"))

valid = (
    df_employees
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-08-03"))) & F.col("job_title").isNotNull())
    .withColumn("job_title_lower", F.lower("job_title"))
    .withColumn("hire_date_minus_65d", F.date_sub("hire_date", 65))
)

with_dims = (
    valid
    .alias("src")
    .join(df_employees.alias("e"), F.col("src.manager_id") == F.col("e.id"), "left")
    .select(
        "src.id",
        "src.full_name",
        "src.email",
        "src.manager_id",
        "src.salary",
        "src.hire_date",
        "src.level",
        "src.hire_date_minus_65d",
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.hire_date").alias("employee_hire_date"),
    )
)

joined = (
    with_dims
    .alias("src")
    .join(df_employees.alias("p"), F.col("src.manager_id") == F.col("p.id"), "left")
    .filter(F.col("p.salary").between(61000, 78000))
    .select(
        "src.*",
        F.col("p.email").alias("employee_email"),
        F.col("p.salary").alias("employee_salary"),
        F.col("p.hire_date").alias("employee_hire_date_2"),
    )
)

ranked = (
    joined
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_dims_employees = (
    ranked
    .alias("s")
    .join(df_employees.alias("e"), F.col("s.manager_id") == F.col("e.id"), "left")
    .select("s.*", F.col("e.job_title"))
)

wide = (
    with_dims_employees
    .groupBy("manager_id")
    .pivot("level", ["L1", "L3", "L5"])
    .agg(F.sum("employee_salary"))
)

result = (
    wide
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select("manager_id", "L1", "L5", F.col("min_employee_salary"))
)
