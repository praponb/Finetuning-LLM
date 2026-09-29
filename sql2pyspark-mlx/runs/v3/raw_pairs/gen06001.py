from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") != 182)
    .select("id", "store_id", "quantity")
    .unionByName(
        df_inventory
        .filter(F.col("store_id").isin([1, 5]))
        .select("id", "store_id", "quantity")
    )
    .distinct()
)
