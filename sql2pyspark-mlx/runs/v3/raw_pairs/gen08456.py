from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") & F.col("email").like("%@mail.co.th") & (F.year("hire_date") >= 2023))
    .withColumn("job_title_filled", F.coalesce("job_title", F.lit("Unknown")))
    .withColumn("full_name_normalized", F.upper(F.trim("full_name")))
)
