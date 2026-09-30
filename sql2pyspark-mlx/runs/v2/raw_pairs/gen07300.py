from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .groupBy(F.date_format(F.col("s.opened_date"), "yyyy-MM").alias("opened_month"))
    .agg(
        F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"),
        F.max(F.col("i.quantity")).alias("max_quantity"),
    )
    .filter(F.col("max_quantity") >= 120)
    .orderBy(F.col("avg_quantity").desc(), "opened_month")
    .limit(5)
)
