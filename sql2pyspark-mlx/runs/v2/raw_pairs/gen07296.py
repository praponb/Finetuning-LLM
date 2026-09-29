from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("stock_qty").between(234, 266)).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.city"),
        F.col("s.supplier_name"),
    )
    .orderBy("country", "id")
    .limit(5)
)
