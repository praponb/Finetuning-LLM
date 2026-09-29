from pyspark.sql import functions as F

base = df_employees.filter(F.col("level").isNotNull() & F.col("is_remote"))

is_remote_summary = (
    base
    .groupBy("is_remote")
    .agg(
        F.min("hire_date").alias("min_hire_date"),
        F.count(F.when(F.col("job_title") == "Director", 1)).alias("director_count"),
    )
)

result = (
    is_remote_summary
    .select("is_remote", "min_hire_date", "director_count")
    .orderBy(F.col("director_count").desc(), "is_remote")
    .limit(50)
)
