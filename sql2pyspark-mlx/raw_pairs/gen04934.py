from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") != "Berlin") | F.col("location").isin(["Chiang Mai", "Bangkok", "London"]))
    .select(
        "dept_name",
        "budget",
        F.substring("location", 1, 3).alias("location_prefix"),
        F.concat_ws("-", "dept_name", "location").alias("dept_name_location_key"),
    )
    .orderBy("dept_name")
)
