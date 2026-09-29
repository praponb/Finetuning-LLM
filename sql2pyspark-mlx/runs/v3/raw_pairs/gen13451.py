from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.year("hire_date") == 2024) & (F.col("manager_id") == 39) & ~F.col("full_name").like("%FRANK%"))
    .select(
        "id",
        "job_title",
        "full_name",
        F.length("full_name").alias("full_name_length"),
    )
    .orderBy("full_name")
)
