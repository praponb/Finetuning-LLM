from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").asc_nulls_last(), "id")
w2 = Window.partitionBy("order_id").orderBy("unit_price_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("order_id", "product_id")
w_recent = Window.partitionBy("order_id").orderBy(F.col("running_min_quantity").asc_nulls_last())

filtered = (
    df_order_items
    .filter(F.col("product_id") == 28)
    .withColumn("unit_price_rounded", F.round("unit_price", 2))
)

deduped = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

scored = (
    deduped
    .filter(F.col("unit_price_rounded") <= 41)
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price_rounded",
        "rn",
        F.avg("quantity").over(w2).alias("running_avg_quantity"),
        F.min("quantity").over(w2).alias("running_min_quantity"),
        F.count("quantity").over(w2).alias("running_count_quantity"),
    )
)

matched_keys = (
    scored
    .select("id", "order_id")
    .intersectAll(
        df_order_items
        .filter(F.col("order_id").isin([7, 25, 37]))
        .select("id", "order_id")
    )
)

verified = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "order_id",
        "unit_price_rounded",
        "running_min_quantity",
        "running_count_quantity",
        F.max("unit_price_rounded").over(w_hist).alias("order_id_max_unit_price_rounded"),
        F.cume_dist().over(w_recent).alias("running_min_quantity_cume_dist"),
    )
)
