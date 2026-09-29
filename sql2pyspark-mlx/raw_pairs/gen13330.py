from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy(F.col("unit_price_int").desc(), "id")
w2 = Window.partitionBy("order_id", "product_id").orderBy(F.col("unit_price_int").desc_nulls_last(), "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("unit_price_int").asc_nulls_last(), "id")

valid = (
    df_order_items
    .filter(F.col("unit_price") <= 170)
    .select(
        "id",
        "order_id",
        "product_id",
        F.col("unit_price").cast("int").alias("unit_price_int"),
    )
)

deduped = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

valid_order_items = (
    deduped
    .filter(F.col("order_id").isin([15, 16, 29, 33]))
    .withColumn("unit_price_int_rounded", F.round("unit_price_int", 0))
)

latest = (
    valid_order_items
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .select("id", "order_id", "unit_price_int", "unit_price_int_rounded")
)

deduped_order_items = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

base = (
    deduped_order_items
    .filter(F.col("unit_price_int") >= 78)
    .select(
        "id",
        "unit_price_int",
        "unit_price_int_rounded",
        "rank_in_group",
        (F.col("unit_price_int") - F.col("rank_in_group")).alias("unit_price_int_minus_rank_in_group"),
    )
)

result = (
    base
    .select(
        "id",
        "unit_price_int",
        "unit_price_int_rounded",
        "rank_in_group",
        "unit_price_int_minus_rank_in_group",
    )
    .orderBy(F.col("rank_in_group").desc(), "unit_price_int_rounded")
)
