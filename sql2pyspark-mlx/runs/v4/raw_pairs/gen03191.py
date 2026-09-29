from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "outer")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("s.sqft") != 4755)
    .groupBy(F.col("s.region"), F.year(F.col("s.opened_date")).alias("opened_year"))
    .agg(F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"))
    .orderBy("opened_year", "region")
    .limit(50)
)
