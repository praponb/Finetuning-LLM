from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .filter(F.col("quantity") < 171)
    .groupBy("store_id")
    .agg(F.countDistinct("quantity").alias("unique_stores"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .filter(F.col("st.unique_stores") > 7)
    .select(F.col("s.id"), F.col("s.city"), F.col("st.unique_stores"))
    .orderBy(F.col("id").desc())
    .limit(3)
)
