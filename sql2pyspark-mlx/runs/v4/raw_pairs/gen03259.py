from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") >= 104)
    .select("product_id", "store_id")
    .unionByName(
        df_inventory
        .filter(F.col("quantity") < 122)
        .select("product_id", "store_id")
    )
    .distinct()
)
