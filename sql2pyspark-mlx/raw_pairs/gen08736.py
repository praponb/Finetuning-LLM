from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") < 132000) & F.col("job_title").isin(["Intern", "Designer", "Manager"]))
    .select("id", "salary")
)
