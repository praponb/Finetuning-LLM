from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "budget",
        "dept_name",
        "location",
        F.trim("dept_name").alias("clean_dept_name"),
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
    )
)
