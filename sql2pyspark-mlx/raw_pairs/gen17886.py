from pyspark.sql import functions as F

max_quantity = df_inventory.agg(F.max("quantity")).first()[0]

result = (
    df_inventory
    .filter(F.col("quantity") > max_quantity)
    .select("id", "quantity", "product_id", "last_restock")
    .orderBy(F.col("quantity").desc(), "id")
    .limit(3)
)
