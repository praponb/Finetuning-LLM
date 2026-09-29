from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "dept_name",
        "budget",
        "location",
        F.upper("dept_name").alias("dept_name_upper"),
    )
)
