from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([1, 2, 11]) & (F.col("plan") == "basic"))
    .select("id", "end_date", "plan", "customer_id")
)
