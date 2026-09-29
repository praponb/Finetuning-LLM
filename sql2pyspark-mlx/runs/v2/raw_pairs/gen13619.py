from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Berlin") & F.col("dept_name").isNull())
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.round("budget", 0).alias("budget_rounded"),
    )
)
