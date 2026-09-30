from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("adjusted_salary", "id")
w2 = Window.partitionBy("job_title").orderBy(F.col("salary").desc(), "id")
w3 = Window.partitionBy("level", "manager_id").orderBy("adjusted_salary", "id")
w_ordered = Window.partitionBy("level", "job_title").orderBy("salary_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("employee_level")
w6 = Window.partitionBy("job_title")
w7 = Window.partitionBy(F.month("hire_date"), "employee_level").orderBy("salary", "id").rowsBetween(-5, Window.currentRow)
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.min("salary").alias("min_employee_salary"))

valid = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-05-22").cast("date"))
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.round((F.col("salary") * 1.1), 2).alias("adjusted_salary"),
    )
)

enriched = (
    valid
    .alias("src")
    .join(df_employees.alias("b"), F.col("src.manager_id") == F.col("b.id"), "inner")
    .select(
        "src.*",
        F.col("b.full_name").alias("employee_full_name"),
        F.col("b.level").alias("employee_level"),
    )
)

enriched_employees = (
    enriched
    .filter(F.col("full_name").like("%FRANK%"))
    .withColumn("next_adjusted_salary", F.lead("adjusted_salary", 2).over(w))
    .withColumn("adjusted_salary_bucket", F.ntile(5).over(w))
    .withColumn("salary_bucket", F.ntile(10).over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
)

metrics = (
    enriched_employees
    .select(
        "id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        "next_adjusted_salary",
        "adjusted_salary_bucket",
        "seq_no",
        F.first("job_title", ignorenulls=True).over(w_ordered).alias("first_known_job_title"),
        F.count("adjusted_salary").over(w5).alias("employee_level_count_adjusted_salary"),
        F.min("adjusted_salary").over(w6).alias("job_title_min_adjusted_salary"),
        F.avg("salary_bucket").over(w7).alias("salary_bucket_avg_5"),
    )
)

result = (
    metrics
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        "next_adjusted_salary",
        "first_known_job_title",
        "employee_level_count_adjusted_salary",
        F.col("min_employee_salary"),
    )
    .orderBy("id")
)
