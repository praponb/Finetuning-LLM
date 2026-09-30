from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.delivered_date").desc(), F.col("c.id").desc())

latest_match = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.delivered_date") <= F.col("s.order_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_id",
        "s.status",
        "s.channel",
        "s.amount",
        "s.shipping_fee",
        F.col("c.cost").alias("shipment_cost"),
        F.col("c.carrier"),
    )
)

result = (
    latest_match
    .withColumn("amount_tier", F.when(F.col("amount") >= 1410, "PREMIUM").otherwise("STANDARD"))
    .withColumn("shipment_cost_tier", F.when(F.col("shipment_cost") >= 78, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["amount_tier", "shipment_cost_tier"], ["amount_tier"]],
        "amount_tier",
        "shipment_cost_tier",
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("amount_tier").desc())
)
