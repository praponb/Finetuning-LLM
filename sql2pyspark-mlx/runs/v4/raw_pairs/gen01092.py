from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").between(29, 39))
    .select("status")
    .unionByName(
        df_subscriptions
        .filter(F.col("end_date") > "2025-02-01")
        .select("status")
    )
    .orderBy("status")
)
