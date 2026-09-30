from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)
w_ordered = Window.partitionBy("product_id").orderBy("quantity", "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_first())
w_part = Window.partitionBy(F.year("last_restock"), "product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("product_id").orderBy("quantity_avg_2", "id")
w6 = Window.partitionBy("product_id")
w7 = Window.partitionBy(F.year("last_restock"), "product_id").orderBy("quantity_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy(F.date_trunc("WEEK", "last_restock"), "product_id").orderBy("prev_quantity_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("product_id").orderBy("cost", "id").rowsBetween(-3, -1)
w10 = Window.partitionBy("product_id").orderBy("quantity_rounded", "id").rowsBetween(-5, -1)
w11 = Window.orderBy(F.col("quantity_rounded").desc(), "id")
w12 = Window.partitionBy(F.month("last_restock"), "product_id").orderBy("last_known_quantity_avg_2", "id")

metrics = (
    df_inventory
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.avg("quantity").over(w_hist).alias("quantity_avg_2"),
        F.ntile(10).over(w_ordered).alias("quantity_bucket"),
        F.percent_rank().over(w3).alias("quantity_percent_rank"),
        F.sum("quantity").over(w_part).alias("running_quantity"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.lag("quantity_bucket").over(w_recent).alias("prev_quantity_bucket"),
        F.lag("quantity_percent_rank").over(w_recent).alias("prev_quantity_percent_rank"),
        F.row_number().over(w_recent).alias("seq_no"),
    )
)

metrics_inventory = (
    enriched
    .filter(F.col("seq_no") >= 4)
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        "quantity_bucket",
        "running_quantity",
        "prev_quantity_bucket",
        F.max("quantity").over(w6).alias("product_id_max_quantity"),
        F.sum("quantity").over(w7).alias("cumulative_quantity"),
        F.last("quantity_avg_2", ignorenulls=True).over(w8).alias("last_known_quantity_avg_2"),
    )
)

joined = (
    metrics_inventory
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.cost"), F.col("p.price"))
)

valid = (
    joined
    .filter(F.col("prev_quantity_bucket") == 6)
    .select(
        "*",
        (F.col("running_quantity") - F.col("prev_quantity_bucket")).alias("running_quantity_minus_prev_quantity_bucket"),
        F.round("quantity", 0).alias("quantity_rounded"),
    )
)

enriched_inventory = (
    valid
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        "quantity_bucket",
        "prev_quantity_bucket",
        "cumulative_quantity",
        "running_quantity_minus_prev_quantity_bucket",
        "quantity_rounded",
        F.avg("quantity").over(w9).alias("quantity_avg_3_prior"),
        F.avg("cumulative_quantity").over(w10).alias("cumulative_quantity_avg_5_prior"),
        F.row_number().over(w11).alias("event_seq"),
        F.row_number().over(w12).alias("row_seq"),
    )
)

result = (
    enriched_inventory
    .select(
        "id",
        "quantity",
        "last_restock",
        "quantity_bucket",
        "prev_quantity_bucket",
        "cumulative_quantity",
        "running_quantity_minus_prev_quantity_bucket",
        "quantity_rounded",
        "cumulative_quantity_avg_5_prior",
    )
)
