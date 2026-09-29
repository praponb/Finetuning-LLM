from pyspark.sql import functions as F

max_quantity = df_inventory.filter(F.col("last_restock") < "2025-10-24").agg(F.max("quantity")).first()[0]

result = (
    df_inventory
    .filter(F.col("quantity") >= max_quantity)
    .select("id", "quantity")
)
