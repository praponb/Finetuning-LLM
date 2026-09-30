from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("carrier") == "Flash", F.col("cost")).otherwise(0)).alias("flash_cost"),
        F.max("cost").alias("max_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("O"), F.col("o.id") == F.col("O.order_id"), "left")
    .filter(F.col("O.max_cost") >= 46)
    .select(
        F.col("o.store_id"),
        F.col("o.shipping_fee"),
        F.col("O.flash_cost"),
        F.col("O.max_cost"),
    )
    .orderBy(F.col("max_cost").desc(), F.col("flash_cost").desc())
)
