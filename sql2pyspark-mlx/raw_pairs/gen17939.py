from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNull())
    .select("id", "store_id")
    .unionByName(
        df_orders
        .filter(F.col("status") != "SHIPPED")
        .select("id", "store_id")
    )
    .distinct()
)
