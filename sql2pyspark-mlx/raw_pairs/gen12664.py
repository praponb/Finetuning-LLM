from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(~((F.col("s.city") == "Tokyo") & F.col("s.opened_date").between("2023-08-27", "2024-05-23")))
    .select(F.col("i.store_id"), F.col("s.opened_date"), F.col("s.city"))
)
