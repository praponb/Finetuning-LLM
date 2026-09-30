from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id", "status").orderBy("order_date", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("store_id")
w_part = Window.partitionBy("store_id").orderBy(F.col("discount").desc(), "id")
w_grp = Window.orderBy(F.col("shipping_fee").desc(), "id")

with_windows = (
    df_orders
    .select(
        "id",
        "customer_id",
        "status",
        "amount",
        F.sum("amount").over(w).alias("running_amount"),
        F.avg("amount").over(w2).alias("amount_avg_3_prior"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
        F.ntile(4).over(w_part).alias("discount_bucket"),
        F.row_number().over(w_grp).alias("seq_no"),
    )
)

result = (
    with_windows
    .select("id", "customer_id", "status", "amount", "discount_bucket", "seq_no")
    .orderBy(F.col("id").desc_nulls_last(), F.col("discount_bucket").desc())
)
