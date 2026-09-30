from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 2424000)
    .select(
        "id",
        "budget",
        "dept_name",
        "location",
        F.lower("dept_name").alias("dept_name_lower"),
    )
    .orderBy(F.col("budget").desc())
)
