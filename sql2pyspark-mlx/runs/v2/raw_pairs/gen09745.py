from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") != 216)
    .select("last_restock")
    .subtract(
        df_inventory
        .filter(F.col("product_id").isin([1, 3, 5, 6]))
        .select("last_restock")
    )
)
