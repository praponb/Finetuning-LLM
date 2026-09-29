from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("s.status") == "RETURNED")
    .groupBy(
        F.year(F.col("o.order_date")).alias("order_year"),
        F.month(F.col("o.order_date")).alias("order_month"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.col("s.cost")).alias("total_cost"),
    )
    .filter(F.col("total_cost") >= 460)
)
