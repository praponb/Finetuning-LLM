from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") != "HR")
    .select(
        "id",
        "dept_name",
        "location",
        F.substring("location", 1, 3).alias("location_prefix"),
        F.trim("location").alias("clean_location"),
    )
)
