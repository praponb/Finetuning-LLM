from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2023-10-01", "2025-06-03"))
    .groupBy("level")
    .agg(F.countDistinct("salary").alias("unique_levels"))
    .orderBy("level")
)
