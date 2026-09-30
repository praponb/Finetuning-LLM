from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([1, 2, 12]))
    .select("start_date")
    .subtract(
        df_shipments
        .filter(F.col("carrier") == "Flash")
        .select("delivered_date")
    )
    .orderBy("start_date")
)
