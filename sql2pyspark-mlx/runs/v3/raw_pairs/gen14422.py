from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Chiang Mai", "London", "Tokyo"]) | F.col("dept_name").isin(["Sales", "HR"]))
    .select(
        "id",
        "location",
        F.substring("dept_name", 1, 3).alias("dept_name_prefix"),
        F.lower("location").alias("location_lower"),
    )
)
