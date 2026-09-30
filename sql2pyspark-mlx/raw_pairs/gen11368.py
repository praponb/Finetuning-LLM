from pyspark.sql import functions as F

recent_departments = df_departments.filter(F.col("location").isin(["London", "New York", "Tokyo"]))

dept_name_summary = (
    recent_departments
    .groupBy("dept_name")
    .agg(
        F.sum(F.when(F.col("dept_name") == "Legal", 1).otherwise(0)).alias("legal_count"),
    )
)

result = (
    dept_name_summary
    .select("dept_name", "legal_count")
    .orderBy("dept_name")
)
