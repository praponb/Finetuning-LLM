from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["DHL", "Kerry", "UPS"]))
    .select("id", "order_id", "delivered_date")
    .intersect(
        df_shipments
        .filter(F.col("cost") <= 70)
        .select("id", "order_id", "delivered_date")
    )
    .orderBy("order_id")
)
