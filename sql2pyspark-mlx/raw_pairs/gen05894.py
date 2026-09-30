from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(~((F.col("s.delivered_date") < "2024-05-01") & F.col("o.order_date").between("2023-12-01", "2024-05-01")))
    .select(
        F.col("s.cost"),
        F.col("s.order_id"),
        F.col("s.delivered_date"),
        F.col("o.status"),
        F.col("o.channel"),
    )
)
