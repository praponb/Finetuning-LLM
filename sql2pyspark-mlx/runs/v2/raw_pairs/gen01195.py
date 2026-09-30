from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Designer", "Intern"]) & (F.col("full_name").like("%Mia%") | (F.col("salary") >= 97000)))
    .select(
        "job_title",
        "email",
        "manager_id",
        F.initcap(F.trim("level")).alias("level_title"),
    )
    .orderBy("level_title")
)
