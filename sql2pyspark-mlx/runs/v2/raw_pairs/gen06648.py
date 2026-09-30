from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "New York")
    .select(
        "id",
        "location",
        "dept_name",
        "budget",
        F.substring("dept_name", 1, 2).alias("dept_name_prefix"),
    )
)
