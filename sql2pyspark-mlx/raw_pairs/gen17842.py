from pyspark.sql import functions as F

filtered = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-07-07").cast("date")) & F.col("email").like("%@gmail.com"))
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        F.regexp_replace("email", " ", "_").alias("email_slug"),
        F.round((F.col("salary") * 100), 2).alias("adjusted_salary"),
    )
)

result = (
    filtered
    .groupBy("manager_id")
    .pivot("job_title", ["Designer", "Manager", "Intern"])
    .agg(F.avg("adjusted_salary").alias("total"), F.count(F.lit(1)).alias("n"))
    .select(
        "manager_id",
        F.col("Designer_total").alias("designer_total"),
        F.col("Designer_n").alias("designer_n"),
        F.col("Manager_total").alias("manager_total"),
        F.col("Manager_n").alias("manager_n"),
        F.col("Intern_total").alias("intern_total"),
        F.col("Intern_n").alias("intern_n"),
    )
)
