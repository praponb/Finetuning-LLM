from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Berlin", "Singapore"]))
    .select(
        "id",
        "location",
        "dept_name",
        "budget",
        F.lower("location").alias("location_lower"),
    )
)
