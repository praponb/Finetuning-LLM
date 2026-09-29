from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.max("sale_date").alias("last_sale_date"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
        F.col("ss.last_sale_date"),
    )
    .orderBy(F.col("sqft").desc(), "id")
    .limit(20)
)
