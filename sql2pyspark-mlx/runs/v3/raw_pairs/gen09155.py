from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote") & F.col("salary").between(77000, 136000))
    .groupBy("level")
    .agg(F.max("salary").alias("max_salary"))
    .orderBy("level")
)
