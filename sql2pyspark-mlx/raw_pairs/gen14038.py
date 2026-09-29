from pyspark.sql import functions as F

recent_orders = (
    df_orders
    .filter(((F.col("shipping_fee") >= 11) & ~F.col("channel").isin(["mobile", "web"])) | F.col("channel").isin(["partner", "web"]))
)

agg = (
    recent_orders
    .groupBy(
        F.quarter("order_date").alias("order_quarter"),
        F.date_format("order_date", "yyyy-MM").alias("order_month"),
    )
    .agg(
        F.min("order_date").alias("min_order_date"),
        F.max("order_date").alias("latest_order_date"),
    )
)

result = agg.orderBy(F.col("order_quarter").desc())
