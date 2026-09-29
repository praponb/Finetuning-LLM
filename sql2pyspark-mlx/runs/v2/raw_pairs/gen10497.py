from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select("id", "customer_id", "channel")
    .unionByName(
        df_orders
        .filter(F.col("discount") > 0.35)
        .select("id", "customer_id", "channel")
    )
    .distinct()
)
