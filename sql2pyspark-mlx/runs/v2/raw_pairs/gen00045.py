from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Marketing", "HR", "Sales"]) & F.col("location").isin(["New York", "Bangkok"]))
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.min("budget").alias("min_budget"),
    )
    .orderBy(F.col("min_budget").desc())
)
