from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(~F.col("o.status").isin(["COMPLETED", "PENDING"]))
    .groupBy(F.col("s.region"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("o.status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
        F.count(F.col("o.amount")).alias("amount_count"),
    )
)
