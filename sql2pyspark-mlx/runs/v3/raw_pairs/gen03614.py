from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Legal", "Sales", "Marketing"]))
    .groupBy("dept_name", "location")
    .agg(
        F.sum(F.when(F.col("dept_name") == "Legal", 1).otherwise(0)).alias("legal_count"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
)
