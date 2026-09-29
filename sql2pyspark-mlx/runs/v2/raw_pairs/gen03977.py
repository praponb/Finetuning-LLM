from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.array_contains(F.col("p.tags"), "eco") | F.col("s.country").isin(["DE", "TH", "US"]))
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("p.product_name"),
        F.col("p.stock_qty"),
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.col("s.rating"),
    )
    .orderBy("product_name")
)
