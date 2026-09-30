from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.year(F.col("o.order_date")) == 2024)
    .groupBy(F.col("o.channel"))
    .agg(F.count(F.when(F.col("s.carrier") == "UPS", 1)).alias("ups_count"))
    .orderBy(F.col("channel").desc(), F.col("ups_count").desc())
)
