from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Director", "Engineer", "Designer"]))
    .agg(F.countDistinct("salary").alias("unique_managers"))
)
