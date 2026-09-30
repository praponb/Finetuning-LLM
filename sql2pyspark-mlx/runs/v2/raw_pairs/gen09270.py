from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_salary").desc())

result = (
    df_employees
    .filter(F.col("manager_id").isin([7, 21, 34]))
    .groupBy("level")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.count("*").alias("cnt"),
    )
    .withColumn("salary_rank", F.rank().over(w))
)
