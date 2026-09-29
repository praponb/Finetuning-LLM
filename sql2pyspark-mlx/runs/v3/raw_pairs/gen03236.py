from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Engineering", "HR"]))
    .select(
        "dept_name",
        "location",
        F.trim("dept_name").alias("clean_dept_name"),
        F.length("location").alias("location_length"),
    )
    .orderBy("clean_dept_name")
)
