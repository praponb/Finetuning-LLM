from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Director", "Manager"]))
    .withColumn("salary_int", F.col("salary").cast("int"))
    .withColumn("job_title_group", F.when(F.col("job_title").isin(["Engineer", "Analyst"]), "Core").otherwise("Other"))
)
