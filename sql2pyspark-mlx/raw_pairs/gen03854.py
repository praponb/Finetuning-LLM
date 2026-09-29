from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("price", "id")
w3 = Window.partitionBy("product_id").orderBy("last_restock", "id")

valid = (
    df_inventory
    .filter(F.col("quantity") == 209)
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_label"),
    )
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_dims = (
    latest
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.stock_qty"), F.col("p.price"))
)

scored = (
    with_dims
    .filter(F.col("store_id").isin([2, 5]))
    .select(
        "*",
        F.lag("quantity").over(w2).alias("prev_quantity"),
        F.lag("price").over(w2).alias("prev_price"),
        F.lead("price").over(w2).alias("next_price"),
        F.row_number().over(w2).alias("row_seq"),
        F.lag("price", 1).over(w3).alias("prev_price_2"),
    )
)

grouped = (
    scored
    .groupBy(
        F.when(F.col("next_price") >= 1160, "HIGH").otherwise("LOW").alias("next_price_tier"),
    )
    .agg(
        F.percentile_approx("next_price", 0.5).alias("next_price_p50"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.round(F.avg("prev_quantity"), 2).alias("avg_prev_quantity"),
        F.percentile_approx("prev_price_2", [0.25, 0.5, 0.75]).alias("prev_price_2_quartiles"),
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
    )
)

result = (
    grouped
    .select(
        "next_price_tier",
        "next_price_p50",
        "distinct_store_ids",
        "quantity_bit_or",
        "prev_price_2_quartiles",
    )
    .orderBy(F.col("next_price_p50").asc_nulls_first())
)
