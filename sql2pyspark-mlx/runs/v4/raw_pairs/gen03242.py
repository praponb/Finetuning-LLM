from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Singapore", "New York"]) & F.col("location").isin(["Chiang Mai", "Singapore", "New York"]) & (F.col("location") == "Berlin"))
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.substring("dept_name", 1, 2).alias("dept_name_prefix"),
    )
    .orderBy("location")
)
