from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("end_date")
    .intersect(
        df_stores
        .filter(F.col("opened_date") >= "2023-07-01")
        .select("opened_date")
    )
)
