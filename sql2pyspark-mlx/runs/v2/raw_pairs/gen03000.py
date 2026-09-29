from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.cost").isNotNull())
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("s.opened_date"),
        F.col("s.region"),
        F.col("p.product_name"),
        F.col("p.category"),
    )
    .orderBy(F.col("id").desc())
)
