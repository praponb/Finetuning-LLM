from pyspark.sql import functions as F

with_dims = (
    df_inventory
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .filter(~F.col("p.brand").isin(["Acme", "Stark", "Initech"]))
    .select("e.*", F.col("p.product_name"))
)

result = (
    with_dims
    .select("id", "store_id", "product_id", "quantity", "last_restock")
    .orderBy(F.col("last_restock").desc_nulls_last(), "quantity")
)
