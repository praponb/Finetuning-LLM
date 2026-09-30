from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Marketing") & (F.col("location") == "New York"))
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        F.coalesce("location", F.lit("unassigned")).alias("location_filled"),
    )
)
