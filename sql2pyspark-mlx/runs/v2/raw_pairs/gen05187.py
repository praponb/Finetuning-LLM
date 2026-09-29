from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Bangkok", "Chiang Mai", "New York"]) & F.col("dept_name").isNotNull() & (F.col("location") == "Bangkok"))
    .select(
        "id",
        "location",
        "dept_name",
        F.substring("dept_name", 1, 2).alias("dept_name_prefix"),
        F.lower("location").alias("location_lower"),
    )
    .orderBy("location_lower")
)
