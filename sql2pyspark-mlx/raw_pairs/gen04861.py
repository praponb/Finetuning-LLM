from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("line_total").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy("quantity", "id")
w3 = Window.partitionBy("order_id").orderBy("row_num", "id")
w4 = Window.partitionBy("product_id", "order_id").orderBy("unit_price_int", "id").rowsBetween(-3, Window.currentRow)
w_hist = Window.partitionBy("product_id").orderBy("unit_price_int", "id")
w6 = Window.orderBy("line_total", "id")
w7 = Window.partitionBy("product_id").orderBy(F.col("unit_price_int").desc(), "id")
w8 = Window.orderBy(F.col("unit_price_int").desc(), "id")
w9 = Window.partitionBy("order_id").orderBy("dedup_rank", "id")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.max("amount").alias("max_payment_amount"))

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("unit_price_int", F.col("unit_price").cast("int"))
    .withColumn("line_total", (F.col("quantity") + F.col("unit_price")))
)

ranked = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

ranked_order_items = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    ranked_order_items
    .withColumn("prev_line_total", F.lag("line_total").over(w3))
    .withColumn("next_line_total", F.lead("line_total").over(w3))
    .withColumn("next_quantity", F.lead("quantity").over(w3))
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("unit_price_int_avg_3", F.avg("unit_price_int").over(w4))
)

scored_order_items = (
    scored
    .filter(F.col("next_quantity") == 7)
    .withColumn("unit_price_int_bucket", F.ntile(3).over(w_hist))
    .withColumn("line_total_bucket", F.ntile(5).over(w6))
    .withColumn("row_seq", F.row_number().over(w7))
    .withColumn("seq_no", F.row_number().over(w8))
    .withColumn("prev_next_line_total", F.lag("next_line_total", 2).over(w9))
)

enriched = (
    scored_order_items
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.product_id",
        "e.quantity",
        "e.unit_price_int",
        "e.prev_line_total",
        "e.next_line_total",
        "e.event_seq",
        "e.unit_price_int_bucket",
        "e.line_total_bucket",
        "e.row_seq",
        "e.seq_no",
        "e.prev_next_line_total",
        F.col("p.price"),
    )
)

base = (
    enriched
    .filter(F.col("quantity") < 8)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price_int",
        "prev_line_total",
        "line_total_bucket",
        "row_seq",
        "seq_no",
        "prev_next_line_total",
        "price",
        F.col("prev_line_total").cast("int").alias("prev_line_total_int"),
        F.coalesce("prev_next_line_total", F.lit(0)).alias("prev_next_line_total_filled"),
    )
)

summary = (
    base
    .groupBy(
        F.when(F.col("prev_line_total_int") >= 71, "HIGH").otherwise("LOW").alias("prev_line_total_int_tier"),
        "seq_no",
        "order_id",
    )
    .agg(
        F.min("unit_price_int").alias("min_unit_price_int"),
        F.percentile_approx("prev_next_line_total_filled", [0.25, 0.5, 0.75]).alias("prev_next_line_total_filled_quartiles"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.bit_and("quantity").alias("quantity_bit_and_2"),
    )
    .filter(F.col("avg_row_seq") < 1000000)
)

result = (
    summary
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "prev_line_total_int_tier",
        "seq_no",
        "order_id",
        "min_unit_price_int",
        "quantity_bit_and",
        "quantity_bit_and_2",
        F.col("max_payment_amount"),
    )
)
