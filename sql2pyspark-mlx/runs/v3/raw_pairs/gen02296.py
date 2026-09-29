from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Singapore") & (F.col("dept_name") != "Marketing"))
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        F.lower("dept_name").alias("dept_name_lower"),
        F.round("budget", 0).alias("budget_rounded"),
    )
)
