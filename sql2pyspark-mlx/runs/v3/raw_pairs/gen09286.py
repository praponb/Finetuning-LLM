from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.discount").between(0.13, 0.37))
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
    )
    .orderBy("opened_date", "discount", "id")
    .limit(5)
)
