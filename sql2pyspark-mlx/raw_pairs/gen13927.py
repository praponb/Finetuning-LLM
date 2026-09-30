from pyspark.sql import functions as F

min_start_date = df_subscriptions.filter(F.year("end_date") == 2025).agg(F.min("start_date")).first()[0]

result = (
    df_subscriptions
    .filter(F.col("start_date") == min_start_date)
    .select("id", "start_date", "status", "end_date")
)
