from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_cost").desc())

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .groupBy(
        F.when(F.col("carrier").isin(["Flash", "FedEx"]), "core").otherwise("other").alias("carrier_group"),
    )
    .agg(F.sum("cost").alias("total_cost"))
    .withColumn("rnk", F.dense_rank().over(w))
)
