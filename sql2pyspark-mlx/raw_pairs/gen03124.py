from pyspark.sql import functions as F

rollup_totals = (
    df_shipments
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .groupingSets(
        [["order_status", "shipment_status"], ["order_status"], []],
        F.col("p.status").alias("order_status"),
        F.col("a.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("a.cost")).alias("max_cost"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    rollup_totals
    .select(
        "shipment_status",
        "cnt",
        "max_cost",
        "gid",
        F.when(F.col("shipment_status").rlike("^(LOST|DELIVERED)$"), "CORE").otherwise("OTHER_ZONE").alias("shipment_status_zone"),
    )
    .orderBy(F.col("gid").desc())
)
