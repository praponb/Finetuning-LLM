from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter((F.col("p.price") > 440) & (F.col("p.category") != "Grocery"))
    .select(
        F.col("p.supplier_id"),
        F.col("ss.store_id"),
        F.col("ss.sale_date"),
        F.greatest(F.col("p.cost"), F.col("p.price")).alias("greatest_cost_price"),
    )
    .orderBy(F.col("store_id").desc())
)
