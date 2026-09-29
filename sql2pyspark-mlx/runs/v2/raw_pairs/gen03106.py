from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 2533000)
    .agg(F.count(F.when(F.col("dept_name") == "Legal", 1)).alias("legal_count"))
)
