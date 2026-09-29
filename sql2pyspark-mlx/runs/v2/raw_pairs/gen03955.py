from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        "dept_name",
        F.upper("location").alias("location_upper"),
    )
)
