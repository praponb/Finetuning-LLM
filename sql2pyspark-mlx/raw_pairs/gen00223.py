from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.upper(F.trim("product_name")) == "NOISE-CANCEL HEADPHONES")
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("category") == "Grocery", F.col("price")).otherwise(0)).alias("grocery_price"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_totals.alias("supplier_totals"), F.col("suppliers.id") == F.col("supplier_totals.supplier_id"), "inner")
    .filter(F.col("supplier_totals.grocery_price") >= 46)
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.city"),
        F.col("supplier_totals.grocery_price"),
    )
    .orderBy(F.col("id").desc())
)
