from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter((F.col("location") != "Tokyo") & (F.col("location") != "New York"))
)

departments_stats = (
    valid_departments
    .groupBy("dept_name")
    .agg(
        F.countDistinct("budget").alias("unique_dept_names"),
        F.sum(F.when(F.col("dept_name") == "Legal", 1).otherwise(0)).alias("legal_count"),
    )
)

result = departments_stats.select("dept_name", "unique_dept_names", "legal_count")
