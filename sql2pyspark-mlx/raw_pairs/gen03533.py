from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("manager_id")
w2 = Window.partitionBy("manager_id").orderBy("employee_salary_per_salary", "id")

cleaned = (
    df_employees
    .filter(~F.col("is_remote"))
    .select("*", F.regexp_extract("email", "@(.+)$", 1).alias("email_domain"))
)

enriched = (
    cleaned
    .alias("src")
    .join(df_employees.alias("p"), F.col("src.manager_id") == F.col("p.id"), "inner")
    .select(
        "src.*",
        F.col("p.salary").alias("employee_salary"),
        F.col("p.hire_date").alias("employee_hire_date"),
        F.col("p.full_name").alias("employee_full_name"),
    )
)

filtered = (
    enriched
    .filter(F.col("salary").between(126000, 138000))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "hire_date",
        "is_remote",
        "level",
        "employee_salary",
        "employee_hire_date",
        "employee_full_name",
        F.round((F.col("employee_salary") / F.col("salary")), 2).alias("employee_salary_per_salary"),
    )
)

scored = (
    filtered
    .withColumn("manager_id_sum_employee_salary", F.sum("employee_salary").over(w_ordered))
    .withColumn("seq_no", F.row_number().over(w2))
)

result = (
    scored
    .alias("src")
    .join(df_employees.alias("e"), F.col("src.manager_id") == F.col("e.id"), "left")
    .groupingSets(
        [["job_title", "employee_level"], ["job_title"]],
        F.col("e.job_title").alias("job_title"),
        F.col("src.level").alias("employee_level"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("src.seq_no")).alias("max_seq_no"),
        F.grouping_id().alias("level_id"),
    )
)
