from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") > 84)
    .select("id", "last_restock")
    .subtract(
        df_inventory
        .filter(F.col("store_id") == 5)
        .select("id", "last_restock")
    )
    .orderBy("id")
)
