from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_salary").desc())

result = (
    df_employees
    .filter(F.col("dept_id").isin([5, 27, 32, 39]))
    .groupBy(F.month("hire_date").alias("hire_month"))
    .agg(
        F.sum("salary").alias("total_salary"),
        F.count("*").alias("cnt"),
    )
    .withColumn("salary_rank", F.rank().over(w))
    .orderBy(F.col("cnt").desc())
)
