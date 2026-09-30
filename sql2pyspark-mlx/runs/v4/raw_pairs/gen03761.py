from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("d.dept_name").isin(["HR", "Marketing", "Finance"]))
    .select(
        F.col("e.job_title"),
        F.col("e.email"),
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("d.location"),
        F.coalesce(F.col("d.location"), F.lit("unassigned")).alias("location_filled"),
    )
)
