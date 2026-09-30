from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("shipping_fee").asc_nulls_last())
w2 = Window.partitionBy("store_id").orderBy("amount", "id")
w3 = Window.partitionBy("status").orderBy(F.col("shipping_fee").desc(), "id")

with_windows = (
    df_orders
    .filter(F.col("status") == "COMPLETED")
    .select(
        "*",
        F.rank().over(w).alias("shipping_fee_rank"),
        F.lag("discount", 2).over(w2).alias("prev_discount"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "customer_id",
        "order_date",
        "amount",
        "discount",
        "shipping_fee",
        "shipping_fee_rank",
        "prev_discount",
    )
)
