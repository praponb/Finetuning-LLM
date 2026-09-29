from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper("full_name").like("%RAC%") & F.col("dept_id").isin([7, 9, 21, 40]) & F.col("manager_id").isNull())
    .select(
        "id",
        "full_name",
        "level",
        F.coalesce("email", F.lit("unassigned")).alias("email_filled"),
    )
)
