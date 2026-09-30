from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.city"))
    .agg(
        F.sum(F.col("p.price")).alias("total_price"),
        F.min(F.col("p.stock_qty")).alias("min_stock_qty"),
    )
)
