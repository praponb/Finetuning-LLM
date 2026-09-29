from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_month")

result = (
    df_employees
    .filter(F.col("job_title") == "Manager")
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(
        F.sum("salary").alias("total_salary"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_salary", F.sum("total_salary").over(w))
    .orderBy(F.col("cnt").desc())
)
