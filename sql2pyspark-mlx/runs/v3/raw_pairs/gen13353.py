from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .groupBy(
        F.col("s.region"),
        F.quarter(F.col("s.opened_date")).alias("opened_quarter"),
    )
    .agg(
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
        F.count(F.col("i.quantity")).alias("quantity_count"),
    )
    .orderBy(F.col("opened_quarter").desc(), "region")
    .limit(3)
)
