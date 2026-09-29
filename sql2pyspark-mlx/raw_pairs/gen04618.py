from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("created_at", "id", "item_no")
w2 = Window.orderBy(F.col("next_item_no").desc(), "id", "item_no")
w3 = Window.orderBy("item_no", "id", "item_no").rowsBetween(-3, Window.currentRow)
w4 = Window.orderBy(F.col("prev_product_id").desc(), "id", "item_no")
w5 = Window.orderBy("item_no", "id", "item_no")
w6 = Window.orderBy("item_no", "id", "item_no").rowsBetween(-6, -1)
w7 = Window.orderBy("next_item_no_avg_3_filled", "id", "item_no")
w8 = Window.orderBy(F.col("row_seq").asc_nulls_first())

lines = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "item"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        (F.col("pos") + 1).alias("item_no"),
        F.col("item.product_id"),
        F.col("item.price"),
    )
)

metrics = (
    lines
    .select(
        "id",
        "item_no",
        F.lead("item_no").over(w_ordered).alias("next_item_no"),
        F.lag("product_id").over(w_ordered).alias("prev_product_id"),
        F.lead("product_id").over(w_ordered).alias("next_product_id"),
        F.row_number().over(w_ordered).alias("row_seq"),
    )
)

metrics_carts = (
    metrics
    .select(
        "*",
        F.row_number().over(w2).alias("seq_no"),
        F.avg("next_item_no").over(w3).alias("next_item_no_avg_3"),
        F.row_number().over(w4).alias("row_seq_2"),
        F.lag("prev_product_id", 2).over(w5).alias("prev_prev_product_id"),
        F.avg("next_item_no").over(w6).alias("next_item_no_avg_6_prior"),
    )
)

cleaned = (
    metrics_carts
    .filter(F.col("prev_product_id") < 4)
    .select(
        "*",
        (F.col("next_item_no") / F.nullif("item_no", F.lit(0))).alias("next_item_no_per_item_no"),
    )
)

filtered = (
    cleaned
    .filter(F.col("next_item_no_avg_3") > 2)
    .select(
        "id",
        "item_no",
        "prev_product_id",
        "next_product_id",
        "row_seq",
        "seq_no",
        "next_item_no_avg_3",
        "row_seq_2",
        "next_item_no_avg_6_prior",
        "next_item_no_per_item_no",
        F.coalesce("next_item_no_avg_3", F.lit(0)).alias("next_item_no_avg_3_filled"),
    )
)

metrics_carts_2 = (
    filtered
    .select(
        "id",
        "item_no",
        "next_product_id",
        "row_seq",
        "seq_no",
        F.ntile(3).over(w7).alias("next_item_no_avg_3_filled_bucket"),
        F.rank().over(w8).alias("row_seq_rank"),
    )
)

base = (
    metrics_carts_2
    .filter(F.col("next_product_id") == 9)
    .select("*", F.coalesce("row_seq_rank", F.lit(0)).alias("row_seq_rank_filled"))
)

result = (
    base
    .select(
        "id",
        "item_no",
        "next_product_id",
        "next_item_no_avg_3_filled_bucket",
        "row_seq_rank",
        "row_seq_rank_filled",
    )
    .orderBy(F.col("next_product_id").desc())
)
