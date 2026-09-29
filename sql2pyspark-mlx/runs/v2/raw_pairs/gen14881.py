from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title", F.year("hire_date").alias("hire_year"))
    .agg(
        F.sum(F.when(F.col("job_title") == "Engineer", F.col("salary")).otherwise(0)).alias("engineer_salary"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
    .filter(F.col("engineer_salary") > 49)
)
