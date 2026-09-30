from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").isNotNull())
    .select("store_id")
    .unionByName(
        df_orders
        .filter(F.col("channel").isin(["mobile", "partner"]))
        .select("store_id")
    )
)
