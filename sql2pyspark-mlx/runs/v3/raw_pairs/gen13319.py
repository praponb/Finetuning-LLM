from pyspark.sql import functions as F

result = (
    df_order_items
    .select("unit_price")
    .union(
        df_shipments
        .filter(F.col("order_id").isin([10, 11, 30]))
        .select("cost")
    )
)
