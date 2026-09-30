from pyspark.sql import functions as F

src = df_employees.filter((F.col("dept_id") == 12) & F.col("email").like("%@mail.co.th"))

employees_stats = (
    src
    .groupBy(
        F.when(F.col("salary") >= 129000, "high").when(F.col("salary") >= 62000, "mid").otherwise("low").alias("salary_bucket"),
        "is_remote",
    )
    .agg(
        F.sum(F.when(F.col("level") == "L3", F.col("salary")).otherwise(0)).alias("l3_salary"),
    )
)

result = (
    employees_stats
    .select("salary_bucket", "l3_salary")
    .orderBy(F.col("l3_salary").desc(), "salary_bucket")
)
