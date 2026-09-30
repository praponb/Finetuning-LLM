from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .select(
        F.col("ss.units"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("s.rating"),
        F.round(F.col("ss.units"), 0).alias("units_rounded"),
    )
)
