from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(F.when(F.col("budget") >= 3089000, "high").when(F.col("budget") >= 1095000, "mid").otherwise("low").alias("budget_bucket"))
    .pivot("dept_name", ["Finance", "Legal", "HR"])
    .agg(F.count("budget"))
)
