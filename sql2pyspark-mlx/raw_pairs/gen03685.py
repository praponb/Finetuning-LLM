from pyspark.sql import functions as F

recent_orders = (
    df_orders
    .filter((F.col("status") != "CANCELLED") & F.col("amount").between(780, 860))
)

order_quarter_summary = (
    recent_orders
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(F.min("discount").alias("min_discount"))
)

result = order_quarter_summary.select("order_quarter", "min_discount")
