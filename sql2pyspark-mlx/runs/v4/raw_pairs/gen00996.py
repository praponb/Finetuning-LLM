from pyspark.sql import functions as F

result = (
    df_inventory
    .select("store_id", "product_id")
    .subtract(
        df_inventory
        .filter(F.col("store_id") == 5)
        .select("store_id", "product_id")
    )
)
