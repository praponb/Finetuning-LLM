from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@gmail.com"))
    .select(
        "salary",
        "is_remote",
        F.concat("job_title", F.lit(" - "), "level").alias("job_title_level"),
    )
    .orderBy("job_title_level", "salary")
)
