from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("store_id").orderBy(F.col("quantity").desc())
w_ordered_2 = Window.orderBy("last_restock", "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("last_restock_month_end").desc_nulls_last(), "id")

valid = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-12-27").cast("date"))
    .select("*", F.last_day("last_restock").alias("last_restock_month_end"))
)

with_windows = (
    valid
    .select(
        "*",
        F.cume_dist().over(w_ordered).alias("quantity_cume_dist"),
        F.lag("quantity", 1, 0).over(w_ordered_2).alias("prev_quantity"),
    )
)

base = (
    with_windows
    .filter(F.col("quantity") >= 214)
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "last_restock_month_end",
        "quantity_cume_dist",
        (F.col("quantity") + F.col("prev_quantity")).alias("quantity_plus_prev_quantity"),
    )
)

deduped = (
    base
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        "last_restock_month_end",
        "quantity_cume_dist",
        "quantity_plus_prev_quantity",
        "dedup_rank",
    )
    .orderBy(F.col("product_id").desc())
)
