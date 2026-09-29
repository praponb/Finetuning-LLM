from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") >= 14)
    .select("id", "store_id", "status")
    .intersect(
        df_orders
        .filter(F.col("channel") == "partner")
        .select("id", "store_id", "status")
    )
    .orderBy(F.col("status").desc())
)
