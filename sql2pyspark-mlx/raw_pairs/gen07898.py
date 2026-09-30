from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("shipped_date"), "carrier").orderBy("shipped_date", "id")
w2 = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("status")
w4 = Window.orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.avg("cost").alias("avg_shipment_cost"))

metrics = (
    df_shipments
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("cost_avg_2_prior", F.avg("cost").over(w2))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w3)), 4))
    .withColumn("cumulative_cost", F.sum("cost").over(w4))
)

result = (
    metrics
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "event_seq",
        "cost_avg_2_prior",
        F.when(F.col("carrier").rlike("^(Flash|FedEx)$"), "GROUP_A").when(F.col("carrier").rlike("^(DHL|Kerry)$"), "EXTENDED").otherwise("PRIORITY").alias("carrier_zone"),
        F.col("avg_shipment_cost"),
    )
    .orderBy(F.col("id").desc_nulls_last())
)
