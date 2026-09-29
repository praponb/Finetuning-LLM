from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .select(
        "id",
        "job_title",
        "manager_id",
        "salary",
        F.upper("full_name").alias("full_name_upper"),
        F.trim("level").alias("clean_level"),
    )
    .orderBy("full_name_upper")
)
