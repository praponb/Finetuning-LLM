from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNull())
    .select("id", "dept_id", F.regexp_replace("email", " ", "-").alias("email_slug"))
)
