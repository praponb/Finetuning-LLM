from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy(F.col("unit_price").desc_nulls_last())
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id")
w4 = Window.partitionBy("order_id")
w5 = Window.partitionBy("order_id").orderBy(F.col("quantity").desc_nulls_first())
w_grp = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_last())
w7 = Window.orderBy(F.col("unit_price_percent_rank").desc_nulls_first())
w8 = Window.orderBy("unit_price_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("order_id", "product_id").orderBy("quantity_dense_rank", "id")

scored = (
    df_order_items
    .select(
        "*",
        F.percent_rank().over(w).alias("unit_price_percent_rank"),
        F.sum("unit_price").over(w2).alias("cumulative_unit_price"),
        F.round((F.col("quantity") / F.sum("quantity").over(w3)), 4).alias("quantity_share"),
        F.corr("quantity", "unit_price").over(w4).alias("corr_quantity_unit_price"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.corr("quantity", "unit_price").over(w4).alias("corr_quantity_unit_price_2"),
        F.dense_rank().over(w5).alias("quantity_dense_rank"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.cume_dist().over(w_grp).alias("quantity_cume_dist"),
        F.percent_rank().over(w7).alias("unit_price_percent_rank_percent_rank"),
        F.last("quantity_dense_rank", ignorenulls=True).over(w8).alias("last_known_quantity_dense_rank"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w3)), 4).alias("unit_price_share"),
        F.lag("quantity_share", 2, 0).over(w9).alias("prev_quantity_share"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "order_id",
        "quantity",
        "cumulative_unit_price",
        "quantity_share",
        "corr_quantity_unit_price",
        "unit_price_percent_rank_percent_rank",
        "unit_price_share",
    )
)
