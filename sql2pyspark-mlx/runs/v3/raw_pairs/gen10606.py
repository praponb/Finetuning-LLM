from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") < 90)
    .select("id", "store_id", "product_id")
    .unionByName(
        df_inventory
        .filter(F.col("quantity") != 91)
        .select("id", "store_id", "product_id")
    )
)
