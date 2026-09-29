from pyspark.sql import functions as F

result = (
    df_web_events
    .select("page", F.lit("web_event").alias("origin"))
    .union(df_customers.select("segment", F.lit("customer").alias("origin")))
    .distinct()
)
