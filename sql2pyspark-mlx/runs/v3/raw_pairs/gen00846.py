from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .select(
        F.col("s.rating"),
        F.col("s.country"),
        F.col("p.cost"),
        F.col("p.stock_qty"),
        F.col("p.price"),
        F.concat(F.col("s.supplier_name"), F.lit(" - "), F.col("s.country")).alias("supplier_name_country"),
    )
)
