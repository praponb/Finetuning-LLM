from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "id",
        "budget",
        F.concat_ws(",", "dept_name", "location").alias("dept_name_location_key"),
        F.coalesce("dept_name", F.lit("unassigned")).alias("dept_name_filled"),
    )
)
