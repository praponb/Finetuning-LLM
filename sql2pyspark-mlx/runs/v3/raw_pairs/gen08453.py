from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(3144000, 3887000))
    .groupBy("location")
    .agg(F.max("budget").alias("max_budget"))
    .filter(F.col("max_budget") > 3525000)
)
