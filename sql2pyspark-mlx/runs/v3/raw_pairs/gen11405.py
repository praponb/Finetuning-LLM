from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.length("full_name") < 7) & F.col("email").isNotNull())
    .select("id", "level", "job_title", "manager_id")
)
