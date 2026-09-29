from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_cost").desc())

result = (
    df_shipments
    .groupBy(F.month("shipped_date").alias("shipped_month"))
    .agg(F.sum("cost").alias("total_cost"))
    .withColumn("cost_rank", F.rank().over(w))
    .orderBy("shipped_month")
    .limit(10)
)
