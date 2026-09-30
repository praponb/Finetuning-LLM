from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("quantity").desc_nulls_last())
w3 = Window.partitionBy("order_id").orderBy("quantity_bucket", "id")
w4 = Window.partitionBy("order_id")

with_windows = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.ntile(10).over(w).alias("quantity_bucket"),
        F.rank().over(w2).alias("quantity_rank"),
    )
)

base = (
    with_windows
    .filter(F.col("order_id") == 4)
    .select(
        "*",
        F.greatest("quantity_rank", "quantity").alias("greatest_quantity_rank_quantity"),
        F.round("quantity", 2).alias("quantity_rounded"),
    )
)

scored = (
    base
    .select(
        "id",
        "order_id",
        "unit_price",
        "quantity_bucket",
        "quantity_rank",
        "greatest_quantity_rank_quantity",
        F.lead("quantity").over(w3).alias("next_quantity"),
        F.lag("quantity").over(w3).alias("prev_quantity"),
        F.lag("greatest_quantity_rank_quantity").over(w3).alias("prev_greatest_quantity_rank_quantity"),
        F.lead("greatest_quantity_rank_quantity").over(w3).alias("next_greatest_quantity_rank_quantity"),
        F.row_number().over(w3).alias("seq_no"),
        F.round((F.col("quantity_bucket") / F.sum("quantity_bucket").over(w4)), 4).alias("quantity_bucket_share"),
    )
)

enriched = (
    scored
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "inner")
    .filter(F.col("b.amount") > 620)
    .select(
        "src.id",
        "src.order_id",
        "src.quantity_bucket",
        "src.next_greatest_quantity_rank_quantity",
        F.col("b.amount"),
    )
)

report = (
    enriched
    .select("id", "next_greatest_quantity_rank_quantity", "amount")
    .orderBy(
        F.col("next_greatest_quantity_rank_quantity").desc_nulls_last(),
        F.col("id").desc(),
    )
    .limit(10)
)

result = (
    report
    .select("id", "amount")
    .orderBy(F.col("amount").desc())
)
