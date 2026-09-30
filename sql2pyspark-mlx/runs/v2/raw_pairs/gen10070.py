from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Singapore")
    .cube("dept_name", "location")
    .agg(
        F.count("*").alias("num_departments"),
        F.count(F.when(F.col("dept_name") == "Finance", 1)).alias("finance_count"),
        F.sum("budget").alias("total_budget"),
    )
    .orderBy(F.col("num_departments").desc())
)
