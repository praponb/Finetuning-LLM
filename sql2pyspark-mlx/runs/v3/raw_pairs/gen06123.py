from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") != "ACTIVE")
    .select("start_date")
    .union(
        df_stores
        .filter(F.col("opened_date") > "2024-09-01")
        .select("opened_date")
    )
    .distinct()
)
