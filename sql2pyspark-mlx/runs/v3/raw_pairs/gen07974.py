from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .groupBy(
        F.quarter(F.col("o.order_date")).alias("order_quarter"),
        F.col("o.customer_id"),
    )
    .agg(F.min(F.col("s.shipped_date")).alias("min_shipped_date"))
    .orderBy("min_shipped_date")
)
