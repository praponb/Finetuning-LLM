from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(2024000, 2668000))
    .select(
        "id",
        "budget",
        "location",
        F.upper("location").alias("location_upper"),
        F.concat("location", F.lit(" - "), "dept_name").alias("location_dept_name"),
    )
    .orderBy("location_dept_name")
)
