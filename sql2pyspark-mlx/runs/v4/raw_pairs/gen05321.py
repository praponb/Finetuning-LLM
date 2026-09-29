from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter((F.year(F.col("s.opened_date")) >= 2023) | F.col("s.sqft").between(6140, 12112))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("s.region"),
        F.col("s.opened_date"),
        F.date_trunc("MONTH", F.col("i.last_restock")).alias("last_restock_period"),
    )
    .orderBy(F.col("id").desc())
)
