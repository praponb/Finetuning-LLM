from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").isNotNull())
    .select("monthly_fee")
    .intersect(df_products.filter(F.col("cost").isNotNull()).select("cost"))
)
