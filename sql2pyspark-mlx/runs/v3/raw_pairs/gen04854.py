from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("%Car%"))
    .groupBy("level")
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
    .filter(F.col("salary_range") < 73)
)
