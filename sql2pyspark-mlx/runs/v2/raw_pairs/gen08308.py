from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("i.quantity") > 233)
    .groupBy(F.date_format(F.col("s.opened_date"), "yyyy-MM").alias("opened_month"))
    .agg(
        F.countDistinct(F.col("i.quantity")).alias("unique_stores"),
        F.max(F.col("i.quantity")).alias("max_quantity"),
    )
    .orderBy(F.col("max_quantity").desc(), "opened_month")
    .limit(10)
)
