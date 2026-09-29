from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Sales") & F.col("location").isNotNull())
    .groupBy("dept_name")
    .agg(
        F.sum(F.when(F.col("dept_name") == "Marketing", F.col("budget")).otherwise(0)).alias("marketing_budget"),
    )
    .orderBy("dept_name", "marketing_budget")
    .limit(3)
)
