from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") >= 222)
    .select("id", "last_restock", "product_id")
    .unionByName(
        df_inventory
        .filter(F.col("product_id").isin([1, 2, 8, 10]))
        .select("id", "last_restock", "product_id")
    )
)
