from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") < 2843000) & (F.col("budget") <= 1836000))
    .groupBy("dept_name")
    .agg(
        F.sum(F.when(F.col("location") == "London", 1).otherwise(0)).alias("london_count"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_count") > 2)
    .orderBy("dept_name", "total_count")
)
