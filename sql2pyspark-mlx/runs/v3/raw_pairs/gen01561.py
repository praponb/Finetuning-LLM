from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.store_name").isNotNull())
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("ss.store_id"),
        F.col("s.opened_date"),
        F.col("s.city"),
        F.col("s.region"),
        F.coalesce(F.col("ss.units"), F.lit(0)).alias("units_filled"),
    )
    .orderBy("id")
    .limit(50)
)
