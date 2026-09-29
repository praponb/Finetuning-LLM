from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("job_title").isin(["Manager", "Designer"]))
    .select(
        "id",
        "hire_date",
        "email",
        "salary",
        F.coalesce("level", F.lit("N/A")).alias("level_filled"),
    )
)
