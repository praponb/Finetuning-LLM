from pyspark.sql import functions as F

result = (
    df_departments
    .agg(
        F.sum(F.when(F.col("dept_name") == "Legal", 1).otherwise(0)).alias("legal_count"),
        F.count("*").alias("row_count"),
    )
    .orderBy("row_count", "legal_count")
)
