from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.filter(F.month("opened_date").isin([1, 4, 11])).alias("s"), F.col("i.store_id") == F.col("s.id"), "left_semi")
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.store_id"),
        F.col("i.product_id"),
    )
    .orderBy(F.col("product_id").desc(), "id")
    .limit(20)
)
