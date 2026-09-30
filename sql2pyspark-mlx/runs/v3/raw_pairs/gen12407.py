from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.countDistinct("revenue").alias("unique_products"),
        F.countDistinct("revenue").alias("unique_stores"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("S"), F.col("s.id") == F.col("S.store_id"), "left")
    .filter(F.col("S.unique_products") >= 4)
    .select(
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("S.unique_products"),
        F.col("S.unique_stores"),
    )
    .orderBy(F.col("unique_stores").desc())
)
