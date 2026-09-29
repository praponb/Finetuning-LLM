from pyspark.sql import functions as F

result = (
    df_orders
    .select("customer_id", "channel")
    .intersect(
        df_orders
        .filter(F.col("discount").isNotNull())
        .select("customer_id", "channel")
    )
)
