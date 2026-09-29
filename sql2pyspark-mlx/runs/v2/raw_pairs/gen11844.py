from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Bangkok")
    .select(
        "budget",
        "dept_name",
        "location",
        F.upper("location").alias("location_upper"),
        F.upper("dept_name").alias("dept_name_upper"),
    )
    .orderBy("dept_name")
)
