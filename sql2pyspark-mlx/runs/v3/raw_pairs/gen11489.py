from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 2990000)
    .select(
        "budget",
        "location",
        "dept_name",
        F.round((F.col("budget") * 0.85), 2).alias("budget_adjusted"),
    )
)
