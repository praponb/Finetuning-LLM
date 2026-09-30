from pyspark.sql import functions as F

max_end_date = df_subscriptions.agg(F.max("end_date")).first()[0]

result = (
    df_subscriptions
    .filter(F.col("end_date") == max_end_date)
    .select("id", "end_date", "start_date")
)
