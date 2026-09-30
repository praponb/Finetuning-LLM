from pyspark.sql import functions as F

cleaned = (
    df_customers
    .filter(F.col("segment").isNotNull() & (F.col("segment") != "SMB"))
    .select("*", F.col("is_active").cast("int").alias("is_active_flag"))
)

result = (
    cleaned
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.order_date") >= F.col("s.signup_date")), "left_semi")
)
