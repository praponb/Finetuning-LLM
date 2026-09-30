from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.filter(F.upper(F.trim("supplier_name")) == "SIAM PARTS CO").alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_semi")
    .filter(F.col("p.stock_qty") == 349)
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("p.launch_date"),
        F.col("p.category"),
    )
)
