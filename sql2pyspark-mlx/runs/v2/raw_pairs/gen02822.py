from pyspark.sql import functions as F

src = (
    df_employees
    .filter(F.col("dept_id").isin([18, 24, 31]))
    .withColumn("is_remote_flag", F.col("is_remote").cast("int"))
)

employees_stats = (
    src
    .groupBy("is_remote")
    .agg(F.count("is_remote_flag").alias("is_remote_flag_count"))
)

result = (
    employees_stats
    .filter(F.col("is_remote_flag_count") > 4)
    .select("is_remote", "is_remote_flag_count")
    .orderBy("is_remote_flag_count")
)
