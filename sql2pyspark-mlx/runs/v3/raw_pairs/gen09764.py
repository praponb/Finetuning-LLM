from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("brand") == "Umbrella", F.col("price")).otherwise(0)).alias("umbrella_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .select(F.col("s.id"), F.col("s.country"), F.col("st.umbrella_price"))
    .orderBy("umbrella_price")
)
