from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("job_title") != "Director") & ((F.col("manager_id") == 33) | F.col("job_title").isin(["Intern", "Designer", "Director"])))
    .groupBy("is_remote", F.year("hire_date").alias("hire_year"))
    .agg(F.count("*").alias("num_employees"))
    .orderBy(F.col("num_employees").desc())
)
