from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["web", "store", "mobile"]))
    .select("id", "status", "channel")
    .intersect(
        df_orders
        .filter(F.col("store_id").isNotNull())
        .select("id", "status", "channel")
    )
    .orderBy("status")
)
