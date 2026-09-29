from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity")
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id")
w3 = Window.partitionBy("order_id", "product_id")
w4 = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity").desc(), "id")
w5 = Window.partitionBy("product_id").orderBy("unit_price", "id")
w6 = Window.orderBy(F.col("corr_unit_price_quantity").desc(), "id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select("*", F.floor("quantity").alias("quantity_whole"))
)

with_windows = (
    filtered
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "quantity_whole",
        F.cume_dist().over(w).alias("quantity_cume_dist"),
        F.lag("quantity_whole", 1).over(w2).alias("prev_quantity_whole"),
        F.corr("unit_price", "quantity").over(w3).alias("corr_unit_price_quantity"),
        F.ntile(3).over(w4).alias("quantity_bucket"),
    )
)

enriched = (
    with_windows
    .filter(F.col("corr_unit_price_quantity") >= -0.3)
    .select(
        "*",
        F.lead("prev_quantity_whole").over(w5).alias("next_prev_quantity_whole"),
        F.lead("quantity_bucket").over(w5).alias("next_quantity_bucket"),
        F.lag("quantity_bucket").over(w5).alias("prev_quantity_bucket"),
        F.row_number().over(w5).alias("event_seq"),
        F.row_number().over(w6).alias("event_seq_2"),
    )
)

cleaned = (
    enriched
    .filter(F.col("event_seq_2").between(7, 8))
    .select(
        "id",
        "product_id",
        "unit_price",
        "quantity_whole",
        "quantity_cume_dist",
        "prev_quantity_whole",
        "corr_unit_price_quantity",
        "quantity_bucket",
        "prev_quantity_bucket",
        "event_seq",
        "event_seq_2",
        F.greatest("event_seq_2", "prev_quantity_whole").alias("greatest_event_seq_2_prev_quantity_whole"),
    )
)

base = (
    cleaned
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "unit_price",
        "quantity_whole",
        "quantity_cume_dist",
        "prev_quantity_bucket",
        "event_seq_2",
        "greatest_event_seq_2_prev_quantity_whole",
        F.when(F.col("unit_price") >= 430, "High").when(F.col("unit_price") >= 170, "Medium").otherwise("Low").alias("unit_price_band"),
    )
)

base_order_items = (
    base
    .filter(F.col("unit_price").between(220, 320))
    .select("*", F.col("unit_price").cast("int").alias("unit_price_int"))
)

result = (
    base_order_items
    .select(
        "id",
        F.col("unit_price").alias("UNIT_PRICE"),
        F.col("greatest_event_seq_2_prev_quantity_whole").alias("GREATEST_EVENT_SEQ_2_PREV_QUANTITY_WHOLE_VAL"),
        "quantity_cume_dist",
    )
    .unpivot(
        ids=["id"],
        values=["UNIT_PRICE", "GREATEST_EVENT_SEQ_2_PREV_QUANTITY_WHOLE_VAL", "quantity_cume_dist"],
        variableColumnName="metric",
        valueColumnName="value",
    )
)
