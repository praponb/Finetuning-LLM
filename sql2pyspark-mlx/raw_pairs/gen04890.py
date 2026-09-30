from pyspark.sql import functions as F

min_end_date = df_subscriptions.agg(F.min("end_date")).first()[0]

result = (
    df_subscriptions
    .filter(F.col("end_date") == min_end_date)
    .select("id", "end_date", "status")
)
