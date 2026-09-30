from pyspark.sql import functions as F

min_start_date = df_subscriptions.filter(F.month("start_date").isin([6, 7, 12])).agg(F.min("start_date")).first()[0]

result = (
    df_subscriptions
    .filter(F.col("start_date") == min_start_date)
    .select("id", "start_date", "monthly_fee", "customer_id")
)
