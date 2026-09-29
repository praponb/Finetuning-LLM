from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.length("full_name") > 12) & F.col("is_remote"))
    .groupBy(F.year("hire_date").alias("hire_year"))
    .agg(F.max("salary").alias("max_salary"))
)
