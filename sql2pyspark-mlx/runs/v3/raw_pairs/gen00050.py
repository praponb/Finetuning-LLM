from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("delivered_date") >= 2024)
    .select("delivered_date")
    .union(
        df_subscriptions
        .filter(F.col("customer_id").isin([5, 17, 37]))
        .select("start_date")
    )
    .orderBy("delivered_date")
)
