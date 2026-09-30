from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 2823000)
    .select(
        "id",
        "budget",
        F.concat_ws("-", "location", "dept_name").alias("location_dept_name_key"),
    )
)
