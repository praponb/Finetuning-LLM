from pyspark.sql import functions as F

max_quantity = df_inventory.filter(F.col("quantity") != 163).agg(F.max("quantity")).first()[0]

result = (
    df_inventory
    .filter(F.col("quantity") > max_quantity)
    .select("id", "quantity", "store_id", "last_restock")
    .orderBy(F.col("quantity").desc())
)
