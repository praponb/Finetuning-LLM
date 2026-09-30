from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.sum(F.when(F.col("location") == "Berlin", 1).otherwise(0)).alias("berlin_count"),
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_count"),
    )
    .filter(F.col("hr_count") > 9)
)
