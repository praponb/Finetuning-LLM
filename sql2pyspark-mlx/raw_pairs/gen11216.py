from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 1895000)
    .groupBy("dept_name")
    .agg(
        F.sum(F.when(F.col("location") == "Chiang Mai", 1).otherwise(0)).alias("chiang_mai_count"),
    )
    .orderBy("chiang_mai_count", "dept_name")
    .limit(5)
)
