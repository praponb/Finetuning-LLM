from pyspark.sql import functions as F

filtered_employees = (
    df_employees
    .filter(F.length("email") >= 5)
    .withColumn("clean_email", F.trim("email"))
)

agg = (
    filtered_employees
    .groupBy("is_remote")
    .agg(
        F.sum(F.when(F.col("job_title") == "Director", 1).otherwise(0)).alias("director_count"),
    )
)

result = agg.select("is_remote", "director_count")
