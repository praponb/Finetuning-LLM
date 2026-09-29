from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.sum(F.when(F.col("dept_name") == "Legal", F.col("budget")).otherwise(0)).alias("legal_budget"),
    )
    .orderBy("location")
)
