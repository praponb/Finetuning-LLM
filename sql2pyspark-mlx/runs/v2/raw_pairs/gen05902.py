from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .groupBy(F.year("hire_date").alias("hire_year"))
    .agg(F.max("salary").alias("max_salary"))
    .orderBy("hire_year", F.col("max_salary").desc())
)
