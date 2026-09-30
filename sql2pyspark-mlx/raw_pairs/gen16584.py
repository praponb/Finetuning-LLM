from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        F.upper("location").alias("location_upper"),
    )
)
