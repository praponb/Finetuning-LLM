from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy(F.month("started_at"), "device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("shipping_fee").alias("sum_order_shipping_fee"))

with_windows = (
    df_sessions
    .filter(F.col("customer_id").isin([13, 14, 21]))
    .select(
        "*",
        F.first("country", ignorenulls=True).over(w).alias("first_known_country"),
        F.row_number().over(w2).alias("seq_no"),
        F.last("browser", ignorenulls=True).over(w3).alias("last_known_browser"),
    )
)

result = (
    with_windows
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "browser",
        "country",
        "first_known_country",
        "seq_no",
        "last_known_browser",
        F.col("sum_order_shipping_fee"),
    )
    .orderBy(F.col("country").desc())
)
