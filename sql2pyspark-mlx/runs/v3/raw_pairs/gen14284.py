from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 3980000)
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.round("budget", 2).alias("budget_rounded"),
        F.lower("dept_name").alias("dept_name_lower"),
    )
    .orderBy("budget", F.col("location").desc())
)
