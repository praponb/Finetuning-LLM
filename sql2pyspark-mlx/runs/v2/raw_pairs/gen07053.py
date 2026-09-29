from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "right")
    .filter((F.col("p.cost") >= 490) & F.col("ss.units").isNotNull())
    .select(
        F.col("ss.store_id"),
        F.col("ss.units"),
        F.col("p.category"),
        F.col("p.supplier_id"),
        F.col("s.sqft"),
        F.col("s.opened_date"),
    )
    .orderBy(F.col("store_id").desc())
)
