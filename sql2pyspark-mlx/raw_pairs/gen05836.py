from pyspark.sql import functions as F

filtered = (
    df_customers
    .filter(F.col("signup_date") > F.lit("2023-07-03").cast("date"))
    .select("*", F.concat_ws("/", "city", "segment").alias("city_segment_key"))
)

result = (
    filtered
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.end_date") < F.col("s.signup_date")), "left_anti")
)
