from pyspark.sql import functions as F

valid_employees = df_employees.filter((F.col("salary") < 137000) | (F.col("salary") <= 105000))

employees_stats = (
    valid_employees
    .groupBy("level")
    .agg(F.countDistinct("salary").alias("unique_levels"))
)

result = (
    employees_stats
    .filter(F.col("unique_levels") > 10)
    .select("level", "unique_levels")
    .orderBy("level")
)
