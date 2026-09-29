from pyspark.sql import functions as F

result = (
    df_orders
    .alias("e")
    .join(df_stores.alias("dim"), F.col("e.store_id") == F.col("dim.id"), "inner")
    .groupBy(F.col("dim.region").alias("region"), F.col("e.status").alias("status"))
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("e.discount")).alias("min_discount"),
        F.sum(F.col("e.shipping_fee")).alias("total_shipping_fee"),
    )
    .orderBy(F.col("rows_total").desc_nulls_first())
)
