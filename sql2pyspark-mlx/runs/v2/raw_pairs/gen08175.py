from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2023-04-01", "2023-12-01"))
    .select("product_id")
    .subtract(df_inventory.filter(F.col("quantity") > 168).select("product_id"))
    .orderBy("product_id")
)
