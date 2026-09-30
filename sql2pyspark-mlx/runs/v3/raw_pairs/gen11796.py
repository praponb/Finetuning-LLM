from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.launch_date") < "2024-04-01")
    .groupBy(
        F.when(F.col("s.city").isin(["Bangkok", "Singapore"]), "priority").otherwise("other").alias("city_group"),
        F.date_format(F.col("s.opened_date"), "yyyy-MM").alias("opened_month"),
    )
    .agg(
        F.count(F.col("i.quantity")).alias("quantity_count"),
        F.sum(F.col("i.quantity")).alias("total_quantity"),
    )
    .orderBy(F.col("opened_month").desc(), "city_group")
    .limit(3)
)
