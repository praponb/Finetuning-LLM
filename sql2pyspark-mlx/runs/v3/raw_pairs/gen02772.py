from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["London", "Chiang Mai", "Singapore"]) & ((F.col("budget") <= 2709000) | (F.col("budget") > 1514000)))
    .select(
        "id",
        "location",
        "budget",
        "dept_name",
        F.concat_ws(",", "dept_name", "location").alias("dept_name_location_key"),
    )
)
