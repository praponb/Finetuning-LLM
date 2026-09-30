from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.upper(F.trim("product_name")) == "COFFEE MAKER")
    .groupBy("supplier_id")
    .agg(
        F.min("stock_qty").alias("min_stock_qty"),
        F.sum(F.when(F.col("category") == "Grocery", F.col("stock_qty")).otherwise(0)).alias("grocery_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("s.country"),
        F.col("ss.min_stock_qty"),
        F.col("ss.grocery_stock_qty"),
    )
    .orderBy(F.col("rating").desc())
)
