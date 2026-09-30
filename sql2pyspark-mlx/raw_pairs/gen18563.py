from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("order_date", "id")

with_windows = (
    df_orders
    .select(
        "*",
        F.first("shipping_fee", ignorenulls=True).over(w).alias("first_known_shipping_fee"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

result = (
    with_windows
    .withColumn("shipping_fee_tier", F.when(F.col("shipping_fee") >= 40, "PREMIUM").otherwise("STANDARD"))
    .rollup("shipping_fee_tier", "status")
    .agg(
        F.stddev_samp("amount").alias("amount_stddev"),
        F.max(F.when(F.col("status") == "COMPLETED", F.col("first_known_shipping_fee"))).alias("completed_max_first_known_shipping_fee"),
        F.grouping_id().alias("gid"),
    )
)
