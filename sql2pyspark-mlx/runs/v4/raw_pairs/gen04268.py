from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter((F.col("o.amount") > 540) | F.col("s.carrier").isin(["DHL", "FedEx", "UPS"]))
    .select(
        F.col("s.cost"),
        F.col("s.order_id"),
        F.col("s.carrier"),
        F.col("o.order_date"),
    )
    .orderBy("carrier", F.col("cost").desc())
)
