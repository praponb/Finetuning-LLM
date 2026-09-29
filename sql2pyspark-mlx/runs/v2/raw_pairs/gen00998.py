from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 3, 6]))
    .select("id", "product_id")
    .unionByName(
        df_inventory
        .filter(F.col("quantity") < 128)
        .select("id", "product_id")
    )
)
