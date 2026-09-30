from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc_nulls_first(), "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.min("cost").alias("min_shipment_cost"))

deduped = (
    df_order_items
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .join(cost_by_order_id, on="order_id", how="left")
    .select("id", "order_id", "rn", F.col("min_shipment_cost"))
    .orderBy(F.col("order_id").desc_nulls_last())
)
