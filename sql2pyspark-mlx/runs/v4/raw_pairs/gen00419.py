from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("unit_price", "id")
w2 = Window.partitionBy("order_id", "product_id").orderBy("quantity", "id")
w_hist = Window.partitionBy("order_id").orderBy(F.col("seq_no").desc())
w4 = Window.partitionBy("order_id").orderBy(F.col("unit_price_bucket").asc_nulls_first())
w5 = Window.partitionBy("order_id").orderBy(F.col("unit_price_bucket").desc(), "id")
w6 = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("product_id").orderBy("line_total", "id").rowsBetween(-6, -1)
w8 = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id")
w9 = Window.orderBy("running_unit_price_bucket", "id")
w10 = Window.orderBy("seq_no_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("order_id")

cleaned = (
    df_order_items
    .filter(F.col("order_id").isin([19, 21, 37]))
    .select("*", (F.col("quantity") - F.col("unit_price")).alias("line_total"))
)

with_windows = (
    cleaned
    .select(
        "*",
        F.ntile(4).over(w).alias("unit_price_bucket"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

enriched = (
    with_windows
    .withColumn("seq_no_rank", F.rank().over(w_hist))
    .withColumn("unit_price_bucket_percent_rank", F.percent_rank().over(w4))
    .withColumn("seq_no_2", F.row_number().over(w5))
    .withColumn("running_unit_price_bucket", F.sum("unit_price_bucket").over(w6))
    .withColumn("unit_price_bucket_avg_6_prior", F.avg("unit_price_bucket").over(w_ordered))
)

deduped = (
    enriched
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .withColumn("running_unit_price_bucket_bucket", F.ntile(4).over(w9))
    .withColumn("running_seq_no_2", F.sum("seq_no_2").over(w10))
    .withColumn("corr_quantity_unit_price", F.corr("quantity", "unit_price").over(w11))
)
