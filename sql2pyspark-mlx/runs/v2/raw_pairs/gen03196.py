from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "id",
        "budget",
        "location",
        F.concat_ws("-", "location", "dept_name").alias("location_dept_name_key"),
    )
    .orderBy(F.col("id").desc())
)
