from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .filter(F.col("quantity") < 173)
    .groupBy("store_id")
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    df_stores.alias("stores")
    .join(store_totals.alias("StoreTotals"), F.col("stores.id") == F.col("StoreTotals.store_id"), "left")
    .filter(F.col("StoreTotals.avg_quantity") > 75)
    .select(
        F.col("stores.id"),
        F.col("stores.city"),
        F.col("stores.opened_date"),
        F.col("StoreTotals.unique_stores"),
        F.col("StoreTotals.avg_quantity"),
    )
    .orderBy(F.col("id").desc())
)
