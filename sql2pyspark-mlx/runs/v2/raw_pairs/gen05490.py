from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.sum(F.when(F.col("dept_name") == "Finance", 1).otherwise(0)).alias("finance_count"),
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Marketing", 1).otherwise(0))) / F.count("*")), 1).alias("pct_marketing"),
    )
    .orderBy(F.col("pct_marketing").desc(), F.col("finance_count").desc(), "dept_name")
    .limit(5)
)
