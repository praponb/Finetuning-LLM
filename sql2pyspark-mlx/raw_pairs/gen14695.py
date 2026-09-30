from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.year("hire_date").alias("hire_year"))
    .agg(
        F.max("hire_date").alias("last_hire_date"),
        F.count(F.when(F.col("job_title") == "Director", 1)).alias("director_count"),
    )
    .orderBy(F.col("hire_year").desc(), F.col("director_count").desc())
)
