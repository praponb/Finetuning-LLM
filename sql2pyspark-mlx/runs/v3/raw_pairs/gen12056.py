from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "left")
    .filter(F.year(F.col("store_sales.sale_date")) >= 2023)
    .select(
        F.col("store_sales.store_id"),
        F.col("store_sales.units"),
        F.col("stores.opened_date"),
        F.col("stores.sqft"),
    )
    .orderBy(F.col("store_id").desc(), "opened_date")
)
