from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") != 14064)
    .select("opened_date")
    .subtract(df_subscriptions.select("start_date"))
)
