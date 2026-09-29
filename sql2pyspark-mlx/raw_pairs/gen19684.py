from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("discount").desc(), "id")
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("store_id").orderBy("shipping_fee", "id").rowsBetween(-5, -1)

with_windows = (
    df_orders
    .select(
        "id",
        "store_id",
        "status",
        "amount",
        F.ntile(4).over(w).alias("discount_bucket"),
        F.corr("discount", "amount").over(w2).alias("corr_discount_amount"),
        F.avg("discount").over(w3).alias("discount_avg_5_prior"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "status",
        "discount_bucket",
        "discount_avg_5_prior",
        F.when(F.col("status").rlike("^(PENDING|CANCELLED)$"), "GROUP_B").otherwise("CORE").alias("status_zone"),
    )
)
