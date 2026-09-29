from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-07-24").cast("date")) & (F.col("status") == "LOST"))
    .withColumn("delivered_date_month_end", F.last_day("delivered_date"))
)

result = (
    cleaned
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.amount"), F.col("b.status").alias("order_status"))
)
