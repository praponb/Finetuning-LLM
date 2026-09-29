from pyspark.sql import functions as F

valid_employees = (
    df_employees
    .filter((F.length("email") >= 8) & F.col("email").like("%@gmail.com") & (F.col("hire_date") >= "2023-04-19"))
    .withColumn("full_name_filled", F.coalesce("full_name", F.lit("N/A")))
)

summary = (
    valid_employees
    .groupBy("level")
    .agg(F.max("hire_date").alias("latest_hire_date"))
)

result = summary.select("level", "latest_hire_date")
