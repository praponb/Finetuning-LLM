from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") & F.col("full_name").like("Noa%"))
    .groupBy(F.year("hire_date").alias("hire_year"))
    .agg(F.max("hire_date").alias("last_hire_date"))
    .orderBy("last_hire_date")
)
