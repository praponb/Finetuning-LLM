from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.month("hire_date").isin([1, 4, 6]) & (F.length("email") >= 9))
    .groupBy("is_remote")
    .agg(
        F.min("salary").alias("min_salary"),
        F.sum(F.when(F.col("job_title") == "Director", F.col("salary")).otherwise(0)).alias("director_salary"),
    )
)
