from pyspark.sql import functions as F

avg_quantity = df_inventory.filter(F.col("product_id").isin([1, 9, 10])).agg(F.avg("quantity")).first()[0]

result = (
    df_inventory
    .filter(F.col("quantity") > avg_quantity)
    .select("id", "quantity")
)
