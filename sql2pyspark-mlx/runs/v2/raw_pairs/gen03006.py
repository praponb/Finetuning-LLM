from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("s.city").isin(["Singapore", "London", "Bangkok"]))
    .select(
        F.col("s.opened_date"),
        F.col("s.city"),
        F.col("ss.store_id"),
        F.col("ss.revenue"),
        F.col("ss.product_id"),
    )
    .orderBy(F.col("store_id").desc())
)
