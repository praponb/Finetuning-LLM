from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("status") == "RETURNED", F.col("cost")).otherwise(0)).alias("returned_cost"),
        F.min("cost").alias("min_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .filter(F.col("t.min_cost") > 72)
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("t.returned_cost"),
        F.col("t.min_cost"),
    )
    .orderBy(F.col("min_cost").desc())
)
