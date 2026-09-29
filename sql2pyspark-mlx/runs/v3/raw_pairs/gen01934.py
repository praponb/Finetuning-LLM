from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .filter(F.col("product_id").isin([2, 3, 5]))
    .groupBy("store_id")
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.countDistinct("quantity").alias("unique_products"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("st.unique_stores"),
        F.col("st.unique_products"),
    )
)
