from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date")
    .union(
        df_subscriptions
        .filter(F.col("status") == "PAUSED")
        .select("start_date")
    )
    .orderBy("launch_date")
)
