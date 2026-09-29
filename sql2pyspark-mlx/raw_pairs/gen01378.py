from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Singapore")
    .select(
        "id",
        "location",
        "dept_name",
        F.upper("dept_name").alias("dept_name_upper"),
    )
)
