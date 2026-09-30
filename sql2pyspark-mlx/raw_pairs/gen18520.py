from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([4, 25, 28, 33]))
    .select("order_date", "channel")
    .unionByName(
        df_orders
        .filter(F.col("status") == "COMPLETED")
        .select("order_date", "channel")
    )
    .distinct()
    .orderBy("channel")
)
