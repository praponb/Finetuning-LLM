from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary")

valid = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-05-28").cast("date")) & F.col("manager_id").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        F.concat_ws(",", "email", "job_title").alias("email_job_title_key"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("level_clean"),
    )
)

result = (
    valid
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "full_name",
        "manager_id",
        "hire_date",
        "email_job_title_key",
        "level_clean",
    )
)
