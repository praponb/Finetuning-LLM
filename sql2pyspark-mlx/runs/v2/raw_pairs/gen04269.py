from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "is_remote",
        "salary",
        "manager_id",
        F.when(F.col("salary") >= 135000, "High").when(F.col("salary") >= 69000, "Medium").otherwise("Low").alias("salary_tier"),
    )
    .orderBy("manager_id", "id")
    .limit(5)
)
