from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy(F.col("quantity_rounded").desc())
w2 = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id")
w4 = Window.orderBy("quantity_rounded", "id").rowsBetween(-6, -1)
w5 = Window.orderBy("unit_price_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("unit_price_avg_6_prior", "id")
w7 = Window.orderBy("quantity_rounded_percent_rank", "id")
w8 = Window.partitionBy("product_id").orderBy("quantity_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("channel").orderBy(F.col("unit_price").desc(), "id")
w10 = Window.partitionBy("product_id").orderBy("unit_price_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("product_id").orderBy("cumulative_quantity_rounded_2", "id")
w12 = Window.partitionBy("order_id", "product_id")

filtered = (
    df_order_items
    .filter(F.col("order_id").isin([2, 3, 28, 30]))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.col("unit_price").cast("int").alias("unit_price_int"),
        F.round("quantity", 2).alias("quantity_rounded"),
    )
)

scored = (
    filtered
    .filter(F.col("quantity") > 6)
    .withColumn("quantity_rounded_percent_rank", F.percent_rank().over(w_ordered))
    .withColumn("cumulative_quantity_rounded", F.sum("quantity_rounded").over(w2))
    .withColumn("quantity_share", F.round((F.col("quantity") / F.sum("quantity").over(w3)), 4))
    .withColumn("unit_price_avg_6_prior", F.avg("unit_price").over(w4))
    .withColumn("cumulative_quantity_rounded_2", F.sum("quantity_rounded").over(w5))
)

joined = (
    scored
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.discount"), F.col("p.channel"), F.col("p.amount"))
)

with_windows = (
    joined
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price_int",
        "quantity_rounded",
        "quantity_share",
        "cumulative_quantity_rounded_2",
        "discount",
        F.row_number().over(w6).alias("event_seq"),
        F.row_number().over(w7).alias("row_seq"),
        F.last("quantity_rounded_percent_rank", ignorenulls=True).over(w8).alias("last_known_quantity_rounded_percent_rank"),
        F.ntile(5).over(w9).alias("unit_price_bucket"),
        F.sum("discount").over(w10).alias("cumulative_discount"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "product_id",
        "quantity_share",
        "cumulative_quantity_rounded_2",
        "discount",
        F.lag("cumulative_discount").over(w_recent).alias("prev_cumulative_discount"),
        F.lead("last_known_quantity_rounded_percent_rank").over(w_recent).alias("next_last_known_quantity_rounded_percent_rank"),
        F.lag("last_known_quantity_rounded_percent_rank").over(w_recent).alias("prev_last_known_quantity_rounded_percent_rank"),
        F.row_number().over(w_recent).alias("row_seq_2"),
        F.round((F.col("row_seq") / F.sum("row_seq").over(w12)), 4).alias("row_seq_share"),
    )
)
