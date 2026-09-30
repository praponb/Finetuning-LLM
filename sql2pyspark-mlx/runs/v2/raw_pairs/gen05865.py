from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "Sales")
    .groupBy("dept_name")
    .agg(
        F.sum(F.when(F.col("location") == "London", 1).otherwise(0)).alias("london_count"),
    )
    .filter(F.col("london_count") < 48)
    .orderBy(F.col("london_count").desc(), "dept_name")
    .limit(10)
)
