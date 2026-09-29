from pyspark.sql import functions as F

result = (
    df_web_events
    .select("page")
    .subtract(df_customers.select(F.col("city").alias("page")))
)
