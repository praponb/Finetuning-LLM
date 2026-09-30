from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~F.col("status").isin(["PENDING", "SHIPPED"]))
    .select("store_id")
    .unionByName(
        df_orders
        .filter(F.col("channel").isin(["mobile", "store", "partner"]))
        .select("store_id")
    )
)
