from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~F.col("channel").isin(["mobile", "store", "web"]))
    .select("status")
    .subtract(
        df_subscriptions
        .filter(F.month("start_date").isin([5, 6, 8]))
        .select("status")
    )
    .orderBy("status")
)
