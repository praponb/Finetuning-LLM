from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") < 70)
    .select("store_id", "product_id")
    .unionByName(
        df_inventory
        .filter(F.col("quantity") >= 62)
        .select("store_id", "product_id")
    )
    .distinct()
)
