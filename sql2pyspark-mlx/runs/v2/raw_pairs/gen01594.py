from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter(F.col("i.store_id").isin([3, 4]))
    .groupBy(
        F.col("s.region"),
        F.date_trunc("MONTH", F.col("s.opened_date")).alias("opened_month"),
    )
    .agg(
        F.max(F.col("i.quantity")).alias("max_quantity"),
        F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"),
    )
    .filter(F.col("max_quantity") >= 90)
    .orderBy(F.col("max_quantity").desc())
)
