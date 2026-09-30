from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").isNotNull() & (F.col("manager_id").isin([5, 27, 28, 35]) | F.col("job_title").isin(["Intern", "Director", "Designer"])))
    .withColumn("email_level_key", F.concat_ws("-", "email", "level"))
    .orderBy("level")
)
