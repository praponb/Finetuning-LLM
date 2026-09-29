from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@outlook.com"))
    .select("id", "email", "job_title")
    .unionByName(
        df_employees
        .filter(F.col("level").isNotNull())
        .select("id", "email", "job_title")
    )
    .orderBy("id")
)
