from pyspark.sql import functions as F

result = (
    df_inventory
    .select("product_id", "store_id")
    .subtract(
        df_inventory
        .filter(F.col("store_id") == 3)
        .select("product_id", "store_id")
    )
)
