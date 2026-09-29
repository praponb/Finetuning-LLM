from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id", "status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.month("delivered_date"), "status")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.avg("cost").alias("avg_shipment_cost"))

metrics = (
    df_shipments
    .filter(F.col("carrier") != "FedEx")
    .select(
        "id",
        "order_id",
        "cost",
        F.first("carrier", ignorenulls=True).over(w).alias("first_known_carrier"),
        F.sum("cost").over(w2).alias("running_cost"),
        F.round((F.col("cost") / F.sum("cost").over(w3)), 4).alias("cost_share"),
    )
)

result = (
    metrics
    .join(cost_by_order_id, on="order_id", how="left")
    .select("id", "order_id", "cost_share", F.col("avg_shipment_cost"))
    .orderBy(F.col("avg_shipment_cost").asc_nulls_first())
)
