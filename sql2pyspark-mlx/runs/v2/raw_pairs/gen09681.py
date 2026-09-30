from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNull())
    .select("id", "channel", "order_date")
    .unionByName(
        df_orders
        .filter(F.col("status") != "CANCELLED")
        .select("id", "channel", "order_date")
    )
    .orderBy(F.col("order_date").desc())
)
