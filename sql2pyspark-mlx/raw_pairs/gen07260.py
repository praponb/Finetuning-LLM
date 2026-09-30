from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("unit_price").desc(), "id")
w_hist = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("order_id")

deduped = (
    df_order_items
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "order_id", "product_id", "quantity", "unit_price")
)

scored = (
    deduped
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.min("unit_price").over(w_hist).alias("last4_min_unit_price"),
        F.avg("unit_price").over(w_hist).alias("last4_avg_unit_price"),
        F.max("unit_price").over(w_hist).alias("last4_max_unit_price"),
        F.corr("quantity", "unit_price").over(w3).alias("corr_quantity_unit_price"),
    )
)

matched_keys = (
    scored
    .select("id")
    .exceptAll(
        df_order_items
        .filter(F.col("product_id") == 4)
        .select("id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

matched_keys_2 = (
    kept_rows
    .select("id", "product_id")
    .intersect(
        df_order_items
        .filter(F.col("product_id").isin([7, 12]))
        .select("id", "product_id")
    )
)

verified = (
    kept_rows
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

with_dims = (
    verified
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "left")
    .select(
        "e.product_id",
        "e.quantity",
        "e.unit_price",
        "e.last4_min_unit_price",
        "e.last4_max_unit_price",
        "e.corr_quantity_unit_price",
        F.col("b.category"),
        F.col("b.stock_qty"),
        F.col("b.price"),
    )
)

result = (
    with_dims
    .select(
        "product_id",
        "quantity",
        "unit_price",
        "last4_max_unit_price",
        "corr_quantity_unit_price",
        "category",
        "price",
    )
)
