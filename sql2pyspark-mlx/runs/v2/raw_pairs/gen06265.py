from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .filter(F.col("last_restock") <= "2023-12-01")
    .groupBy("store_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("st.quantity_range"),
        F.col("st.stddev_quantity"),
    )
    .orderBy(F.col("stddev_quantity").desc(), "id")
    .limit(10)
)
