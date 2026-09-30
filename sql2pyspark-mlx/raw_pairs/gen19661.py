from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Sales", "Engineering", "HR"]))
    .select(
        "id",
        "budget",
        "location",
        "dept_name",
        F.round("budget", 1).alias("budget_rounded"),
    )
)
