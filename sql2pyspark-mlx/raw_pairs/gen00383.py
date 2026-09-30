from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date")
    .union(
        df_stores
        .filter(F.upper(F.trim("store_name")) == "DOWNTOWN EXPRESS")
        .select("opened_date")
    )
    .distinct()
)
