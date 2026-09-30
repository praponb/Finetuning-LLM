from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(~F.col("p.brand").isin(["Umbrella", "Globex", "Stark"]))
    .groupBy(F.col("p.brand"), F.col("p.category"))
    .agg(
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
        F.max(F.col("i.last_restock")).alias("latest_last_restock"),
    )
)
