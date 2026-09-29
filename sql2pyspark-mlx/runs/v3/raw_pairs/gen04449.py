from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .groupBy(
        F.quarter(F.col("s.opened_date")).alias("opened_quarter"),
        F.col("s.region"),
    )
    .agg(F.sum(F.col("i.quantity")).alias("total_quantity"))
    .filter(F.col("total_quantity") >= 870)
)
