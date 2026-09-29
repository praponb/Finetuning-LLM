from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")
w2 = Window.orderBy(F.col("salary").desc(), "id")
w3 = Window.partitionBy("employee_job_title").orderBy("employee_salary", "id")
w4 = Window.partitionBy("manager_id", "job_title").orderBy(F.col("employee_salary").desc(), "id")

filtered = (
    df_employees
    .filter(F.col("job_title").isNotNull())
    .withColumn("hire_date_label", F.date_format("hire_date", "yyyy-MM"))
)

ranked = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    ranked
    .alias("s")
    .join(df_employees.alias("d"), F.col("s.manager_id") == F.col("d.id"), "inner")
    .select(
        "s.*",
        F.col("d.salary").alias("employee_salary"),
        F.col("d.job_title").alias("employee_job_title"),
        F.col("d.level").alias("employee_level"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.row_number().over(w2).alias("row_seq"),
        F.lag("employee_salary", 1).over(w3).alias("prev_employee_salary"),
    )
)

valid = (
    metrics
    .filter(F.col("manager_id").isNotNull() & ~F.col("job_title").isin(["Intern", "Designer", "Analyst"]))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "hire_date_label",
        "employee_salary",
        "employee_job_title",
        "employee_level",
        "prev_employee_salary",
        F.when(F.col("salary") >= 138000, "High").when(F.col("salary") >= 70000, "Medium").otherwise("Low").alias("salary_band"),
        F.coalesce("job_title", F.lit("UNKNOWN")).alias("job_title_filled"),
    )
)

deduped = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

grouped = (
    deduped
    .groupBy("employee_job_title")
    .agg(
        F.count(F.when(F.col("job_title") == "Analyst", 1)).alias("analyst_count"),
        F.count("*").alias("cnt"),
        F.count_if(F.col("employee_job_title") == "Engineer").alias("engineer_events"),
    )
    .filter(F.col("analyst_count") < 1)
)

result = grouped.select("employee_job_title", "analyst_count", "cnt")
