from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(
        F.sum(F.when(F.col("s.status") == "RETURNED", F.col("s.cost")).otherwise(0)).alias("returned_cost"),
        F.min(F.col("s.shipped_date")).alias("min_shipped_date"),
    )
)
