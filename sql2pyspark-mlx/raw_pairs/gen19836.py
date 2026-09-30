from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "customer_id")
    .intersect(
        df_orders
        .filter(F.col("customer_id").isin([9, 17, 24]))
        .select("id", "customer_id")
    )
)
