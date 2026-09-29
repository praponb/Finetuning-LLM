from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_cost").desc())

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 83, "high").when(F.col("cost") >= 27, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.sum("cost").alias("total_cost"))
    .withColumn("rnk", F.rank().over(w))
    .orderBy(F.col("total_cost").desc())
)
