from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "New York")
    .select(
        "id",
        "budget",
        "location",
        "dept_name",
        F.when(F.col("location") == "London", 1).otherwise(0).alias("is_london"),
    )
)
