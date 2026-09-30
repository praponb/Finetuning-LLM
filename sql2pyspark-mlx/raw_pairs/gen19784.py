from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "order_id").orderBy(F.col("amount").desc(), "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.sum("cost").alias("sum_shipment_cost"))

deduped = (
    df_payments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    deduped
    .join(cost_by_order_id, on="order_id", how="left")
    .select("id", "order_id", "method", "status", F.col("sum_shipment_cost"))
    .orderBy(F.col("order_id").desc())
)
