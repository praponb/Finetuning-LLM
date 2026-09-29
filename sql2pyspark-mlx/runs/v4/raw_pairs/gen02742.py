from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .filter(F.col("quantity").between(127, 196))
    .groupBy("store_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("*").alias("cnt"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .select(F.col("s.region"), F.col("st.avg_quantity"), F.col("st.cnt"))
    .orderBy(F.col("cnt").desc(), F.col("avg_quantity").desc())
)
