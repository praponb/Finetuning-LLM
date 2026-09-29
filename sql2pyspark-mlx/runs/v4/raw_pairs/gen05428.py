from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("channel")

enriched = (
    df_orders
    .select(
        "*",
        F.last("amount", ignorenulls=True).over(w).alias("last_known_amount"),
        F.corr("amount", "discount").over(w_recent).alias("corr_amount_discount"),
    )
)

result = (
    enriched
    .select(
        "id",
        "customer_id",
        "order_date",
        "amount",
        "discount",
        "shipping_fee",
        "last_known_amount",
        "corr_amount_discount",
    )
    .orderBy(F.col("shipping_fee").desc_nulls_first())
)
