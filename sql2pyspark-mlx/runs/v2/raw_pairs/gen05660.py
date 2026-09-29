from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1193000, 2531000) | (F.col("dept_name") == "Engineering"))
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
    )
)
