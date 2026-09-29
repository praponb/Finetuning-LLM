from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Legal") & (F.col("location") == "Singapore") & ~F.col("dept_name").isin(["Sales", "Marketing", "HR"]))
    .agg(F.min("budget").alias("min_budget"))
    .orderBy(F.col("min_budget").desc())
)
