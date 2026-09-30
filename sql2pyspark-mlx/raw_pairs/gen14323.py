from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_cost").desc())

result = (
    df_shipments
    .filter(F.col("status") == "IN_TRANSIT")
    .groupBy(
        F.when(F.col("cost") >= 80, "high").when(F.col("cost") >= 40, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.sum("cost").alias("total_cost"))
    .withColumn("cost_rank", F.dense_rank().over(w))
    .orderBy("cost_bucket")
    .limit(5)
)
