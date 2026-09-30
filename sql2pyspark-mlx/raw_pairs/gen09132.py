from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_credit_limit").desc())

result = (
    df_customers
    .groupBy(
        F.when(F.col("city").isin(["Berlin", "London"]), "tier_1").otherwise("other").alias("city_group"),
    )
    .agg(F.sum("credit_limit").alias("total_credit_limit"))
    .withColumn("credit_limit_rank", F.dense_rank().over(w))
    .orderBy(F.col("total_credit_limit").desc())
)
