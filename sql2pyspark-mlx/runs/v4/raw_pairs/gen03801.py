from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary_bucket")

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 164000, "high").when(F.col("salary") >= 82000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.sum("salary").alias("total_salary"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_salary", F.sum("total_salary").over(w))
)
