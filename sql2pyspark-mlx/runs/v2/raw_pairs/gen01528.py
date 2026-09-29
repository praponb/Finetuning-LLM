from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter((F.col("cost") <= 75) & F.col("status").isin(["IN_TRANSIT", "RETURNED"]) & F.col("order_id").isNotNull())
)

agg = (
    recent_shipments
    .groupBy("carrier", F.quarter("delivered_date").alias("delivered_quarter"))
    .agg(
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.count(F.when(F.col("carrier") == "Kerry", 1)).alias("kerry_count"),
    )
)

result = (
    agg
    .select("carrier", "delivered_quarter", "returned_count", "kerry_count")
    .orderBy("delivered_quarter")
)
