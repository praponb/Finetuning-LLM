from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("carrier") == "Flash", F.col("cost")).otherwise(0)).alias("flash_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("O"), F.col("o.id") == F.col("O.order_id"), "left")
    .filter(F.col("O.flash_cost") >= 64)
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.store_id"),
        F.col("O.flash_cost"),
    )
    .orderBy(F.col("flash_cost").desc())
)
