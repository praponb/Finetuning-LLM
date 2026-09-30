from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("status") == "CANCELLED") & F.month("order_date").isin([6, 9, 10]))
    .select(
        "status",
        "store_id",
        "channel",
        (F.datediff(F.current_date(), "order_date") / 365).alias("years_since_order_date"),
    )
    .orderBy(F.col("years_since_order_date").desc())
)
