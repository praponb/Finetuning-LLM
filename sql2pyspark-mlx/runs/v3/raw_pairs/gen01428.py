from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Finance") & (F.col("budget") >= 2820000))
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        F.coalesce("dept_name", F.lit("unassigned")).alias("dept_name_filled"),
        F.concat_ws("-", "location", "dept_name").alias("location_dept_name_key"),
    )
)
