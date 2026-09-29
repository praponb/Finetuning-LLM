from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy("order_date", "id")
w2 = Window.orderBy(F.col("shipping_fee").desc_nulls_last())
revenue_by_store_id = df_store_sales.groupBy("store_id").agg(F.avg("revenue").alias("avg_store_sale_revenue"))

with_windows = (
    df_orders
    .select(
        "*",
        F.lead("shipping_fee").over(w).alias("next_shipping_fee"),
        F.lag("shipping_fee").over(w).alias("prev_shipping_fee"),
        F.lag("amount").over(w).alias("prev_amount"),
        F.row_number().over(w).alias("event_seq"),
        F.rank().over(w2).alias("shipping_fee_rank"),
    )
)

result = (
    with_windows
    .join(revenue_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "prev_amount",
        F.col("avg_store_sale_revenue"),
    )
    .orderBy(F.col("store_id").asc_nulls_last())
)
