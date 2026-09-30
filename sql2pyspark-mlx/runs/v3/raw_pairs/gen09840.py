from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~((F.upper(F.trim("full_name")) == "FRANK MILLER") | F.col("job_title").isin(["Director", "Engineer", "Manager"])))
    .groupBy("job_title")
    .agg(F.sum("salary").alias("total_salary"))
    .filter(F.col("total_salary") >= 683000)
)
