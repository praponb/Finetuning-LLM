from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("s.sqft") >= 11920)
    .groupBy(
        F.date_trunc("MONTH", F.col("s.opened_date")).alias("opened_month"),
        F.col("s.region"),
    )
    .agg(F.sum(F.col("i.quantity")).alias("total_quantity"))
)
