from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .filter(F.col("status") == "LOST")
    .groupBy("order_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "LOST", 1).otherwise(0))) / F.count("*")), 1).alias("pct_lost"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "left")
    .filter(F.col("ot.pct_lost") > 75)
    .select(F.col("o.status"), F.col("o.channel"), F.col("ot.pct_lost"))
    .orderBy("status")
)
