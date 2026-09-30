from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote") & F.col("dept_id").isin([6, 16, 21, 37]) & F.col("level").isin(["L5", "L1", "L2"]))
    .select(
        "id",
        "full_name",
        "manager_id",
        "job_title",
        F.regexp_extract("email", "@(.+)$", 1).alias("email_domain"),
        F.concat("full_name", F.lit(" - "), "job_title").alias("full_name_job_title"),
    )
    .orderBy(F.col("id").desc())
)
