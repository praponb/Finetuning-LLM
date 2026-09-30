from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("hire_date") > "2024-01-13") & F.col("is_remote"))
    .select(
        "full_name",
        F.when(F.col("job_title") == "Analyst", 1).otherwise(0).alias("is_analyst"),
        F.round("salary", 2).alias("salary_rounded"),
    )
    .orderBy(F.col("is_analyst").desc(), F.col("salary_rounded").desc())
)
