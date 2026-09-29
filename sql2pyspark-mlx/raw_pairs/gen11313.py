from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Marketing", "Legal", "HR"]) & ~F.col("dept_name").isin(["Sales", "Legal"]))
    .groupBy("location")
    .agg(
        F.count("*").alias("num_departments"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
    .orderBy("num_departments", "avg_budget")
)
