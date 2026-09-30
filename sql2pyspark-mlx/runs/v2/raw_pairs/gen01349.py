from pyspark.sql import functions as F

result = (
    df_departments
    .agg(
        F.sum(F.when(F.col("dept_name") == "Marketing", F.col("budget")).otherwise(0)).alias("marketing_budget"),
    )
    .orderBy("marketing_budget")
)
