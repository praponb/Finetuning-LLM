from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") < "2023-12-01")
    .select("cost")
    .union(df_subscriptions.select("monthly_fee"))
    .distinct()
    .orderBy("cost")
)
