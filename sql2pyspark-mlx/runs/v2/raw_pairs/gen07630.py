from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.countDistinct("price").alias("unique_brands"),
        (F.max("price") - F.min("price")).alias("price_range"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "left")
    .filter(F.col("st.unique_brands") >= 9)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("st.unique_brands"),
        F.col("st.price_range"),
    )
    .orderBy(F.col("id").desc())
)
