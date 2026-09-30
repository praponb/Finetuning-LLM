from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Marketing", "HR"]))
    .select(
        "location",
        F.concat_ws(",", "dept_name", "location").alias("dept_name_location_key"),
    )
)
