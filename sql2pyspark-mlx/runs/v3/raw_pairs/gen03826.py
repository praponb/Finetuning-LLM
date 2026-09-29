from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([9, 18, 32]) & F.col("status").isin(["IN_TRANSIT", "DELIVERED", "RETURNED"]))
    .groupBy("carrier")
    .agg(
        F.count("cost").alias("cost_count"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
    .orderBy("carrier")
    .limit(50)
)
