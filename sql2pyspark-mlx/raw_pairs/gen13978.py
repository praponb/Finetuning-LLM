from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
amount_by_store_id = df_orders.groupBy("store_id").agg(F.sum("amount").alias("sum_order_amount"))

latest_match = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_id",
        "s.status",
        "s.amount",
        "s.shipping_fee",
        F.col("c.delivered_date"),
    )
)

result = (
    latest_match
    .join(amount_by_store_id, on="store_id", how="left")
    .select("id", "store_id", "status", "shipping_fee", F.col("sum_order_amount"))
    .orderBy("sum_order_amount")
)
