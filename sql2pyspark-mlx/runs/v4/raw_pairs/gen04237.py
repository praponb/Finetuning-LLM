from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
    )
)
