from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .agg(F.count(F.when(F.col("job_title") == "Designer", 1)).alias("designer_count"))
    .filter(F.col("designer_count") > 2)
    .orderBy(F.col("designer_count").desc())
)
