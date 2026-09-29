from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("stock_qty") > 158)
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("brand") == "Acme", 1).otherwise(0)).alias("acme_count"),
        F.count(F.when(F.col("category") == "Home", 1)).alias("home_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("st.acme_count"),
        F.col("st.home_count"),
    )
    .orderBy(F.col("country").desc())
)
