from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_salary").desc())

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 126000, "high").when(F.col("salary") >= 42000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(F.sum("salary").alias("total_salary"))
    .withColumn("rnk", F.dense_rank().over(w))
)
