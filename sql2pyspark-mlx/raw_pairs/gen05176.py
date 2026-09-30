from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("store_id").orderBy(F.col("quantity_filled").desc_nulls_first())
w3 = Window.partitionBy("product_id", "store_id").orderBy("quantity_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id", "product_id").orderBy("quantity_filled_percent_rank", "id")

filtered = (
    df_inventory
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-05-17")))
    .select(
        "*",
        F.round("quantity", 2).alias("quantity_rounded"),
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

with_dims = (
    filtered
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.cost") < 280)
    .select("a.*", F.col("p.price"))
)

with_windows = (
    with_dims
    .select(
        "*",
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
        F.percent_rank().over(w2).alias("quantity_filled_percent_rank"),
        F.last("quantity_filled", ignorenulls=True).over(w3).alias("last_known_quantity_filled"),
    )
)

with_windows_inventory = (
    with_windows
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity_filled",
        "price",
        "price_share",
        "quantity_filled_percent_rank",
        "last_known_quantity_filled",
        F.lag("quantity_filled").over(w4).alias("prev_quantity_filled"),
        F.lead("quantity_filled").over(w4).alias("next_quantity_filled"),
        F.lead("last_known_quantity_filled").over(w4).alias("next_last_known_quantity_filled"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

result = (
    with_windows_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "price",
        "price_share",
        "last_known_quantity_filled",
        "prev_quantity_filled",
        "next_quantity_filled",
        "next_last_known_quantity_filled",
    )
)
