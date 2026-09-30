from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 3419000)
    .select(
        "id",
        "budget",
        "location",
        "dept_name",
        F.lower("dept_name").alias("dept_name_lower"),
    )
)
