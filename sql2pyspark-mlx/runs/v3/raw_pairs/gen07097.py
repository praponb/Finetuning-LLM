from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.coalesce("dept_name", F.lit("unassigned")).alias("dept_name_filled"),
    )
)
