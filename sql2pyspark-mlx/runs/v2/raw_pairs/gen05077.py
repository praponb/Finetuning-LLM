from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "location",
        "budget",
        "dept_name",
        F.substring("dept_name", 1, 3).alias("dept_name_prefix"),
    )
)
