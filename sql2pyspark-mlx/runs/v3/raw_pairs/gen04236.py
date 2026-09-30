from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").isNull())
    .select("id", "order_date", "store_id")
    .unionByName(
        df_orders
        .filter(F.col("status") != "RETURNED")
        .select("id", "order_date", "store_id")
    )
)
