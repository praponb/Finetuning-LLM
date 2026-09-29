from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.stock_qty") == 400)
    .groupBy(
        F.quarter(F.col("p.launch_date")).alias("launch_quarter"),
        F.col("p.category"),
    )
    .agg(
        F.round(F.stddev(F.col("ss.units")), 2).alias("stddev_units"),
        F.min(F.col("ss.sale_date")).alias("min_sale_date"),
    )
    .orderBy(F.col("min_sale_date").desc())
)
