from pyspark.sql import functions as F

min_quantity = df_inventory.filter(F.col("last_restock") < "2024-02-01").agg(F.min("quantity")).first()[0]

result = (
    df_inventory
    .filter(F.col("quantity") < min_quantity)
    .select("id", "quantity")
)
