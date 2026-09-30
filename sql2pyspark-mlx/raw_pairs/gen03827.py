from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(68000, 94000) | (F.col("salary") <= 120000))
    .groupBy("level", F.month("hire_date").alias("hire_month"))
    .agg(F.countDistinct("salary").alias("unique_managers"))
    .filter(F.col("unique_managers") > 4)
    .orderBy(F.col("unique_managers").desc())
)
