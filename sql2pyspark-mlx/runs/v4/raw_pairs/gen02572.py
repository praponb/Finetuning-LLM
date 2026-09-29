from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level") == "L3")
    .groupBy("is_remote", "job_title")
    .agg(
        F.count(F.when(F.col("job_title") == "Director", 1)).alias("director_count"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
    .filter(F.col("salary_range") > 75)
    .orderBy(F.col("job_title").desc(), "is_remote")
    .limit(50)
)
