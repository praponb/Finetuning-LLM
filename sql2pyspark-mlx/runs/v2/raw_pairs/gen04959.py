from pyspark.sql import functions as F

min_start_date = df_subscriptions.filter(F.col("plan").isin(["basic", "standard", "enterprise"])).agg(F.min("start_date")).first()[0]

result = (
    df_subscriptions
    .filter(F.col("start_date") == min_start_date)
    .select("id", "start_date", "end_date")
    .orderBy("end_date", "id")
    .limit(1)
)
