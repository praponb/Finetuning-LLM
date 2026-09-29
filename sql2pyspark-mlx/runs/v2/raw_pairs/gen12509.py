from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("hire_date") > "2025-07-07") & (F.col("manager_id") == 12))
    .withColumn("hire_date_label", F.date_format("hire_date", "yyyy-MM"))
    .withColumn("salary_whole", F.floor("salary"))
)
