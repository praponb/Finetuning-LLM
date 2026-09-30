from pyspark.sql import functions as F

result = (
    df_products
    .select("cost")
    .union(
        df_subscriptions
        .filter(~F.col("status").isin(["ACTIVE", "CANCELLED"]))
        .select("monthly_fee")
    )
)
