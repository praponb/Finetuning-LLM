from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.brand") == "Globex")
    .select(
        F.col("s.rating"),
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.col("p.cost"),
        F.col("p.stock_qty"),
    )
    .orderBy("supplier_name")
)
