from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "full_name",
        "hire_date",
        F.when(F.col("job_title").isin(["Director", "Manager"]), "Core").otherwise("Other").alias("job_title_group"),
        F.upper("level").alias("level_upper"),
    )
)
