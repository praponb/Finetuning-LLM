from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.min("sale_date").alias("first_sale_date"))
)

result = (
    df_stores.alias("stores")
    .join(store_totals.alias("store_totals"), F.col("stores.id") == F.col("store_totals.store_id"), "left")
    .select(
        F.col("stores.id"),
        F.col("stores.sqft"),
        F.col("store_totals.first_sale_date"),
    )
    .orderBy(F.col("first_sale_date").desc(), "sqft")
)
