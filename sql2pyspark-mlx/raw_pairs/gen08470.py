from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "order_date",
        (F.datediff(F.current_date(), "order_date") / 365).alias("years_since_order_date"),
    )
    .orderBy("order_date", "id")
)
