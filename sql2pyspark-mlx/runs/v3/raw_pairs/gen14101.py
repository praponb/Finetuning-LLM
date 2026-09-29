from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([7, 12, 14, 25]))
    .select(
        "job_title",
        "level",
        F.when(F.col("level").isin(["L3", "L1"]), "Group A").otherwise("Other").alias("level_group"),
        F.lower("job_title").alias("job_title_lower"),
    )
    .orderBy(F.col("job_title").desc())
)
