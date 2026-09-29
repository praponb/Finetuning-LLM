from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") <= "2023-04-02") & ((F.col("quantity") >= 176) | F.col("product_id").isin([1, 2, 9, 10])))
    .select("id", "last_restock", "quantity")
)
