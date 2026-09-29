from pyspark.sql import functions as F

rollup_totals = (
    df_orders
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["region", "city"], ["region"], []],
        F.col("p.region").alias("region"),
        F.col("p.city").alias("city"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("s.shipping_fee")), 2).alias("avg_shipping_fee"),
        F.round(F.sum(F.col("s.amount")), 2).alias("total_amount"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    rollup_totals
    .select("city", "num_rows", "total_amount")
    .orderBy(F.col("total_amount").desc_nulls_first())
)
