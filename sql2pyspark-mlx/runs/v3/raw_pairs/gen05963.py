from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "outer")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .groupBy(
        F.col("s.region"),
        F.quarter(F.col("s.opened_date")).alias("opened_quarter"),
    )
    .agg(
        F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"),
        F.max(F.col("i.last_restock")).alias("latest_last_restock"),
    )
)
