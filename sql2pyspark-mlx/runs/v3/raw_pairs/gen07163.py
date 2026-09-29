from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["FedEx", "Flash"]))
    .select("shipped_date")
    .union(
        df_orders
        .filter(F.col("customer_id").isNotNull())
        .select("order_date")
    )
)
