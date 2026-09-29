from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_cost").desc())

result = (
    df_shipments
    .filter(F.col("order_id") == 17)
    .groupBy("carrier")
    .agg(F.sum("cost").alias("total_cost"))
    .withColumn("cost_rank", F.rank().over(w))
)
