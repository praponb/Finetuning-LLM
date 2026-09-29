from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary").between(119000, 139000) & (F.col("dept_id") == 19)) | F.col("is_remote"))
    .select(
        "id",
        "salary",
        "manager_id",
        F.when(F.col("job_title") == "Analyst", 1).otherwise(0).alias("is_analyst"),
    )
)
