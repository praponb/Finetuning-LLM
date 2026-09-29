from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("category") == "Electronics")
    .groupBy("supplier_id")
    .agg(F.min("price").alias("min_price"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .filter(F.col("st.min_price") > 950)
    .select(F.col("s.supplier_name"), F.col("s.rating"), F.col("st.min_price"))
    .orderBy("min_price", "supplier_name")
)
