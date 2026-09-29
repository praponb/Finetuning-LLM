from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_salary").desc())

result = (
    df_employees
    .filter(F.lower("email").like("%ob.%"))
    .groupBy(F.year("hire_date").alias("hire_year"))
    .agg(F.sum("salary").alias("total_salary"))
    .withColumn("salary_rank", F.dense_rank().over(w))
    .orderBy(F.col("salary_rank").desc(), "hire_year")
    .limit(10)
)
