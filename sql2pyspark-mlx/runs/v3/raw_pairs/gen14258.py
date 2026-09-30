from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_salary").desc())

result = (
    df_employees
    .filter(F.col("full_name").like("%Pim%"))
    .groupBy("manager_id")
    .agg(F.sum("salary").alias("total_salary"))
    .withColumn("rnk", F.dense_rank().over(w))
    .orderBy(F.col("rnk").desc(), "manager_id")
    .limit(5)
)
