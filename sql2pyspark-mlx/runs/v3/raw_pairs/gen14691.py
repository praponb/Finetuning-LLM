from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .groupBy(F.quarter(F.col("p.launch_date")).alias("launch_quarter"))
    .agg(
        (F.max(F.col("ss.units")) - F.min(F.col("ss.units"))).alias("units_range"),
        F.max(F.col("ss.sale_date")).alias("last_sale_date"),
    )
)
