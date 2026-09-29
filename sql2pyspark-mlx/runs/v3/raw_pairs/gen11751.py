from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.filter(F.col("opened_date").between("2023-07-01", "2023-08-01")).alias("s"), F.col("i.store_id") == F.col("s.id"), "left_anti")
    .select(F.col("i.id"), F.col("i.quantity"))
    .orderBy("id")
)
