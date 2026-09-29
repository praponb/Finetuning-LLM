from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(~F.col("s.city").isin(["Chiang Mai", "Berlin"]) & (F.col("p.stock_qty") < 294))
    .select(
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.col("s.city"),
        F.col("p.product_name"),
        F.col("p.price"),
        F.upper(F.trim(F.col("s.supplier_name"))).alias("supplier_name_normalized"),
    )
)
