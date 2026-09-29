from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "EXPIRED")
    .select("start_date")
    .unionByName(
        df_subscriptions
        .filter(F.col("monthly_fee") > 190)
        .select("start_date")
    )
    .orderBy(F.col("start_date").desc())
)
