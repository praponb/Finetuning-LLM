from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") != "HR") & F.col("location").isin(["Chiang Mai", "Tokyo"]) & (F.col("budget") < 1276000))
    .select(
        "id",
        "budget",
        F.concat_ws("-", "dept_name", "location").alias("dept_name_location_key"),
    )
)
