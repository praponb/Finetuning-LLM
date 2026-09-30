from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.delivered_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.date_trunc("week", "order_date"), "status").orderBy("shipment_cost", "id")
w3 = Window.orderBy(F.col("shipment_cost").desc(), "id")
w_part = Window.partitionBy("customer_id").orderBy("shipment_cost", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy("shipment_cost", "id")
w_hist = Window.partitionBy("status").orderBy(F.col("shipment_cost").desc(), "id")

latest_match = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.order_date",
        "s.status",
        "s.channel",
        F.col("c.cost").alias("shipment_cost"),
        F.col("c.status").alias("shipment_status"),
    )
)

eligible = (
    latest_match
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.quantity") < 7), "left_semi")
)

scored = (
    eligible
    .select(
        "*",
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w3).alias("seq_no"),
        F.avg("shipment_cost").over(w_part).alias("shipment_cost_avg_6"),
        F.lead("shipment_cost", 2).over(w5).alias("next_shipment_cost"),
        F.ntile(5).over(w_hist).alias("shipment_cost_bucket"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.order_id") == 32), "left_anti")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (~F.col("c.carrier").isin(["FedEx", "Kerry"])), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "shipment_cost",
        "shipment_status",
        "row_seq",
        "shipment_cost_avg_6",
        F.when(F.col("channel").rlike("^(web|store)$"), "EXTENDED").when(F.col("channel").rlike("^(mobile|partner)$"), "OTHER_ZONE").otherwise("CORE").alias("channel_zone"),
    )
    .orderBy(F.col("channel_zone").desc())
)
