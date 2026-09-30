from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Sales", "Legal"]))
    .groupBy(F.when(F.col("budget") >= 4064000, "high").when(F.col("budget") >= 1099000, "mid").otherwise("low").alias("budget_bucket"))
    .pivot("dept_name", ["Engineering", "Sales", "Marketing", "Legal"])
    .agg(F.max("budget"))
)
