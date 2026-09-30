from pyspark.sql import functions as F

result = (
    df_departments
    .agg(
        F.sum(F.when(F.col("dept_name") == "Finance", 1).otherwise(0)).alias("finance_count"),
    )
    .orderBy(F.col("finance_count").desc())
)
