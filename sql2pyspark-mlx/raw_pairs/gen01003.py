from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 5, 6, 10]))
    .groupBy("store_id")
    .agg(
        F.min("sale_date").alias("min_sale_date"),
        F.countDistinct("units").alias("unique_stores"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .select(
        F.col("s.opened_date"),
        F.col("s.city"),
        F.col("st.min_sale_date"),
        F.col("st.unique_stores"),
    )
    .orderBy("city")
)
