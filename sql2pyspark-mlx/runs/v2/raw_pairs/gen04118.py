from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNull())
    .select("id", "status", "order_date")
    .unionByName(
        df_orders
        .filter(F.col("store_id").isNull())
        .select("id", "status", "order_date")
    )
    .orderBy("status")
)
