from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id")
w2 = Window.orderBy("unit_price", "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")
w4 = Window.partitionBy("order_id")
w5 = Window.partitionBy("order_id").orderBy(F.col("next_unit_price").asc_nulls_first())
w6 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("product_id").orderBy(F.col("event_seq_share").asc_nulls_first())
w8 = Window.partitionBy("product_id").orderBy(F.col("quantity").asc_nulls_last())
w9 = Window.partitionBy("product_id").orderBy("next_unit_price_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("product_id")
w11 = Window.partitionBy("product_id").orderBy("first_known_unit_price", "id")
w12 = Window.partitionBy("product_id").orderBy("quantity", "id")

scored = (
    df_order_items
    .select(
        "*",
        F.lead("unit_price", 2).over(w).alias("next_unit_price"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

metrics = (
    scored
    .withColumn("event_seq_share", F.round((F.col("event_seq") / F.sum("event_seq").over(w4)), 4))
    .withColumn("next_unit_price_dense_rank", F.dense_rank().over(w5))
    .withColumn("first_known_unit_price", F.first("unit_price", ignorenulls=True).over(w6))
)

filtered = (
    metrics
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        "next_unit_price",
        "event_seq",
        "event_seq_share",
        "first_known_unit_price",
        F.col("next_unit_price").cast("int").alias("next_unit_price_int"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.percent_rank().over(w7).alias("event_seq_share_percent_rank"),
        F.rank().over(w8).alias("quantity_rank"),
        F.last("quantity", ignorenulls=True).over(w9).alias("last_known_quantity"),
    )
)

result = (
    with_windows
    .withColumn("next_unit_price_share", F.round((F.col("next_unit_price") / F.sum("next_unit_price").over(w10)), 4))
    .withColumn("first_known_unit_price_bucket", F.ntile(10).over(w11))
    .withColumn("quantity_rank_share", F.round((F.col("quantity_rank") / F.sum("quantity_rank").over(w10)), 4))
    .withColumn("seq_no", F.row_number().over(w12))
)
