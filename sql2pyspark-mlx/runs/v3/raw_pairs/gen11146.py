from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([2, 5, 10]))
    .select("cost")
    .union(df_subscriptions.select("monthly_fee"))
    .distinct()
    .orderBy(F.col("cost").desc())
)
