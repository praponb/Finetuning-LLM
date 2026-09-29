from pyspark.sql import functions as F

cost_by_order_id = df_shipments.groupBy("order_id").agg(F.max("cost").alias("max_shipment_cost"))

kept_keys = (
    df_shipments
    .select("id", "status")
    .exceptAll(
        df_shipments
        .filter(F.col("order_id").isin([10, 22, 26]))
        .select("id", "status")
    )
)

reconciled = (
    df_shipments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.when(F.col("status").rlike("^(RETURNED|LOST)$"), "OTHER_ZONE").otherwise("CORE").alias("status_zone"),
        F.col("max_shipment_cost"),
    )
)
