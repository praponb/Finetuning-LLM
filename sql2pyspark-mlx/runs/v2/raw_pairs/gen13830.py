from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") < 1560)
    .select("status", "customer_id")
    .unionByName(
        df_orders
        .filter(F.col("shipping_fee").between(28, 32))
        .select("status", "customer_id")
    )
)
