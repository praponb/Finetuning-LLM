from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 1357000)
    .agg(F.sum("budget").alias("total_budget"))
    .orderBy(F.col("total_budget").desc())
)
