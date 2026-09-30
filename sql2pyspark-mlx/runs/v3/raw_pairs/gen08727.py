from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id") == 15)
    .select("status")
    .unionByName(
        df_subscriptions
        .filter(F.col("monthly_fee").between(70, 130))
        .select("status")
    )
)
