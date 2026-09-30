from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("job_title") != "Engineer") & (F.col("dept_id").isin([10, 17, 26, 40]) | F.col("full_name").isNotNull()))
    .withColumn("salary_level", F.when(F.col("salary") >= 134000, "High").when(F.col("salary") >= 81000, "Medium").otherwise("Low"))
    .withColumn("email_full_name_key", F.concat_ws("-", "email", "full_name"))
)
