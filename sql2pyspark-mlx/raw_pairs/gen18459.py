from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNotNull())
    .agg(F.count(F.when(F.col("dept_name") == "Legal", 1)).alias("legal_count"))
)
