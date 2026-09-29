from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.filter(F.col("product_id").isin([1, 6, 9, 10])).alias("i"), F.col("s.id") == F.col("i.store_id"), "left_anti")
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("s.region"),
    )
    .orderBy(F.col("id").desc(), F.col("store_name").desc())
)
