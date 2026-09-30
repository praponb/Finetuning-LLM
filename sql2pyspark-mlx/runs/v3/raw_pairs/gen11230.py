from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~((F.col("budget") < 2689000) & F.col("location").isin(["New York", "Chiang Mai", "London"])))
    .groupBy("dept_name")
    .agg(F.min("budget").alias("min_budget"))
)
