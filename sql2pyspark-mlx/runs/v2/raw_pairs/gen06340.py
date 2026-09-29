from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .intersect(
        df_subscriptions
        .filter(F.col("plan").isin(["enterprise", "standard"]))
        .select("status")
    )
)
