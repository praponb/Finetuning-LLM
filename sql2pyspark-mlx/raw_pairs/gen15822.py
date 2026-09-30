from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("unit_price").asc_nulls_last())
w3 = Window.partitionBy("order_id", "product_id").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("seq_no", "id")
unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.avg("unit_price").alias("avg_order_item_unit_price"))

scored = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.row_number().over(w).alias("seq_no"),
        F.dense_rank().over(w2).alias("unit_price_dense_rank"),
    )
)

base = (
    scored
    .filter(F.col("product_id").isNotNull())
    .withColumn("greatest_seq_no_quantity", F.greatest("seq_no", "quantity"))
)

with_windows = (
    base
    .select(
        "*",
        F.sum("greatest_seq_no_quantity").over(w3).alias("running_greatest_seq_no_quantity"),
        F.lead("quantity", 1, 0).over(w4).alias("next_quantity"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("greatest_seq_no_quantity") > 46)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "unit_price_dense_rank",
        "running_greatest_seq_no_quantity",
        "next_quantity",
        F.col("unit_price").cast("int").alias("unit_price_int"),
    )
)

result = (
    cleaned
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "unit_price_dense_rank",
        "running_greatest_seq_no_quantity",
        "next_quantity",
        "unit_price_int",
        F.col("avg_order_item_unit_price"),
    )
    .orderBy(F.col("next_quantity").desc_nulls_first())
)
