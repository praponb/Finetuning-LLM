from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("hire_date") > "2024-08-01") & ~F.col("is_remote") & F.col("is_remote"))
    .groupBy("level")
    .agg(F.sum(F.when(F.col("level") == "L2", 1).otherwise(0)).alias("l2_count"))
    .orderBy(F.col("l2_count").desc())
)
