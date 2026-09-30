from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "location",
        F.coalesce("dept_name", F.lit("Unknown")).alias("dept_name_filled"),
        F.trim("dept_name").alias("clean_dept_name"),
    )
)
