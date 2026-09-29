from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 1754000)
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        F.lower("dept_name").alias("dept_name_lower"),
    )
)
