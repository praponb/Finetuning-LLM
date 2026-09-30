from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") > 140000) & (F.length("email") > 8))
    .groupBy("is_remote")
    .agg(
        F.sum(F.when(F.col("job_title") == "Director", F.col("salary")).otherwise(0)).alias("director_salary"),
    )
    .filter(F.col("director_salary") > 29)
)
