from pyspark.sql import functions as F

result = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.browser").isNotNull()), "left_semi")
)
