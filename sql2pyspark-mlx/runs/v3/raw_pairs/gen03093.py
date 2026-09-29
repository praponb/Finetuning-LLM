from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .select(
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.col("p.brand"),
        F.col("p.stock_qty"),
        F.length(F.col("s.country")).alias("country_length"),
    )
    .orderBy(F.col("country_length").desc(), F.col("supplier_name").desc())
)
