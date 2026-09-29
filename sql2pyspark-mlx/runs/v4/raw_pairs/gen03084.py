from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "HR") & (F.col("budget") > 1492000))
    .select(
        "budget",
        "location",
        F.concat_ws("-", "dept_name", "location").alias("dept_name_location_key"),
    )
    .orderBy(F.col("budget").desc())
)
