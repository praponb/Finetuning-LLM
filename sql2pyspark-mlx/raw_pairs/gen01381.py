from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Finance", "HR"]))
    .select(
        "location",
        "dept_name",
        F.coalesce("location", F.lit("unassigned")).alias("location_filled"),
    )
)
