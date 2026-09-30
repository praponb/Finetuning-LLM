from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "CANCELLED")
    .select("store_id", "channel")
    .unionByName(
        df_orders
        .filter(F.col("channel").isin(["partner", "store", "web"]))
        .select("store_id", "channel")
    )
    .orderBy("channel")
)
