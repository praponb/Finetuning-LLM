from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "outer")
    .groupBy(F.date_trunc("MONTH", F.col("s.opened_date")).alias("opened_month"))
    .agg(F.countDistinct(F.col("i.quantity")).alias("unique_stores"))
    .orderBy(F.col("unique_stores").desc())
)
