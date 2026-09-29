from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") > 2813000) & (F.col("dept_name") == "HR"))
    .select(
        "id",
        "dept_name",
        "location",
        F.round("budget", 2).alias("budget_rounded"),
        F.coalesce("location", F.lit("unassigned")).alias("location_filled"),
    )
)
