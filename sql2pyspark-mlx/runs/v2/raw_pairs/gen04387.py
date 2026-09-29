from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .filter(F.col("sale_date") < "2023-08-11")
    .groupBy("store_id")
    .agg(F.countDistinct("units").alias("unique_products"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .filter(F.col("st.unique_products") >= 4)
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("st.unique_products"),
    )
    .orderBy(F.col("id").desc())
)
