from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "right")
    .filter(F.col("s.sqft").between(12242, 15955) & F.col("s.region").isin(["East", "West"]))
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("ss.product_id"),
        F.col("ss.store_id"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.floor(F.col("ss.units")).alias("units_whole"),
    )
    .orderBy(F.col("units_whole").desc())
)
