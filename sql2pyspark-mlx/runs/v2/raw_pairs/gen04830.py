from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .filter(F.col("product_id").isin([1, 2, 5]))
    .groupBy("store_id")
    .agg(F.countDistinct("quantity").alias("unique_products"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "left")
    .filter(F.col("st.unique_products") > 5)
    .select(F.col("s.id"), F.col("s.sqft"), F.col("st.unique_products"))
    .orderBy(F.col("sqft").desc())
)
