from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(2916000, 3209000) & F.col("location").isin(["Bangkok", "Berlin"]) & F.col("location").isNotNull())
    .agg(F.min("budget").alias("min_budget"))
)
