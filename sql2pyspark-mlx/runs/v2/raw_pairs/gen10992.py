from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("hire_date") > "2023-05-01") & F.lower("full_name").like("%aro%") & ~F.col("is_remote"))
    .groupBy("is_remote")
    .agg(
        F.sum(F.when(F.col("level") == "L1", F.col("salary")).otherwise(0)).alias("l1_salary"),
        F.count(F.when(F.col("level") == "L5", 1)).alias("l5_count"),
    )
)
