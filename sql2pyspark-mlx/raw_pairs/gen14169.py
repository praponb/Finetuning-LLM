from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("hire_date") < "2024-01-01") & F.col("is_remote"))
    .select(
        "id",
        "manager_id",
        "salary",
        F.concat("level", F.lit("/"), "job_title").alias("level_job_title"),
        F.when(F.col("job_title").isin(["Engineer", "Manager"]), "Tier 1").otherwise("Other").alias("job_title_group"),
    )
)
