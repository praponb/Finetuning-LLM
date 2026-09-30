from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.filter(F.col("cost").between(220, 230)).alias("p"), F.col("i.product_id") == F.col("p.id"), "left_semi")
    .filter(F.col("i.quantity") == 115)
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.store_id"),
        F.col("i.quantity"),
    )
    .orderBy("store_id", F.col("quantity").desc(), "id")
    .limit(50)
)
