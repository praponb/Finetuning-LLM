from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 1983000)
    .select(
        "id",
        "dept_name",
        "budget",
        F.length("location").alias("location_length"),
        F.lower("dept_name").alias("dept_name_lower"),
    )
    .orderBy(F.col("budget").desc(), "id")
    .limit(5)
)
