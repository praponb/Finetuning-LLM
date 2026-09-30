from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.id"), F.col("s.region"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        (F.max(F.col("o.amount")) - F.min(F.col("o.amount"))).alias("amount_range"),
        F.round(((100 * F.sum(F.when(F.col("o.channel") == "store", 1).otherwise(0))) / F.count("*")), 1).alias("pct_store"),
    )
    .orderBy(F.col("pct_store").desc())
)
