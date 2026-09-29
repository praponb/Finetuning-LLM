from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.max("sale_date").alias("latest_sale_date"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .filter(F.col("st.stddev_units") > 52)
    .select(
        F.col("s.opened_date"),
        F.col("s.store_name"),
        F.col("st.stddev_units"),
        F.col("st.latest_sale_date"),
    )
    .orderBy("stddev_units")
)
