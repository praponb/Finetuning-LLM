from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("hire_date") < "2025-05-29") & F.col("dept_id").isin([7, 33]) & F.col("full_name").like("%Chen%"))
    .select(
        "full_name",
        "is_remote",
        F.coalesce("full_name", F.lit("unassigned")).alias("full_name_filled"),
    )
    .orderBy("full_name_filled")
)
