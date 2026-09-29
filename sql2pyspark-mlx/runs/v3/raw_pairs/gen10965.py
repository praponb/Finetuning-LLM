from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2023-05-30", "2024-06-04"))
    .groupBy("level")
    .agg(F.countDistinct("salary").alias("unique_depts"))
    .orderBy("level")
)
