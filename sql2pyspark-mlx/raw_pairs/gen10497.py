from pyspark.sql import functions as F

cost_by_order_id = df_shipments.groupBy("order_id").agg(F.avg("cost").alias("avg_shipment_cost"))

kept_keys = (
    df_shipments
    .select("carrier")
    .exceptAll(
        df_shipments
        .filter(F.col("delivered_date") < "2023-08-01")
        .select("carrier")
    )
)

verified = (
    df_shipments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.col("avg_shipment_cost"),
    )
    .orderBy(F.col("shipped_date").asc_nulls_first())
)
