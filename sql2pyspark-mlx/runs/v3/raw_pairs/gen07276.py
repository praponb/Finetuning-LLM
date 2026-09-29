from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Singapore")
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        F.concat_ws("-", "dept_name", "location").alias("dept_name_location_key"),
        F.concat_ws("-", "location", "dept_name").alias("location_dept_name_key"),
    )
    .orderBy("location_dept_name_key")
)
