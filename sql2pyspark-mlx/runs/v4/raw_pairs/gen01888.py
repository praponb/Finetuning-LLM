from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Finance", "Engineering"]) & (F.col("budget") < 2160000))
    .select(
        "id",
        "budget",
        F.length("location").alias("location_length"),
        F.when(F.col("dept_name") == "Engineering", 1).otherwise(0).alias("is_engineering"),
    )
)
