from pyspark.sql import functions as F

max_quantity = df_inventory.filter(F.col("product_id").isin([5, 10])).agg(F.max("quantity")).first()[0]

result = (
    df_inventory
    .filter(F.col("quantity") > max_quantity)
    .select("id", "quantity", "last_restock")
    .orderBy(F.col("id").desc(), "last_restock")
    .limit(3)
)
