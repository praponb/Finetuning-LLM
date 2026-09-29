from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.array_contains("tags", "sale"))
    .groupBy("supplier_id")
    .agg(
        (F.max("price") - F.min("price")).alias("price_range"),
        F.sum(F.when(F.col("brand") == "Acme", F.col("price")).otherwise(0)).alias("acme_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("s.country"),
        F.col("st.price_range"),
        F.col("st.acme_price"),
    )
    .orderBy(F.col("id").desc())
)
