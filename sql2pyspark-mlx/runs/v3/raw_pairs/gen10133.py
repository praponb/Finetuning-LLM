from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") & (F.upper("full_name").like("%OAH%") | (F.length("email") >= 14)))
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.sum(F.when(F.col("job_title") == "Manager", F.col("salary")).otherwise(0)).alias("manager_salary"),
    )
)
