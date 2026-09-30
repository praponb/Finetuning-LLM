from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 3, 4, 6]))
    .groupBy("store_id")
    .agg(
        (F.max("units") - F.min("units")).alias("units_range"),
        F.min("sale_date").alias("first_sale_date"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "left")
    .filter(F.col("st.units_range") > 66)
    .select(
        F.col("s.id"),
        F.col("s.sqft"),
        F.col("st.units_range"),
        F.col("st.first_sale_date"),
    )
    .orderBy(F.col("id").desc())
)
