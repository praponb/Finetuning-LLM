from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id")
w3 = Window.orderBy("running_min_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("running_max_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("prev_unit_price", "id").rowsBetween(-6, -1)

scored = (
    df_order_items
    .select(
        "id",
        "quantity",
        F.max("quantity").over(w).alias("running_max_quantity"),
        F.sum("quantity").over(w).alias("running_sum_quantity"),
        F.min("quantity").over(w).alias("running_min_quantity"),
        F.lag("unit_price", 1, 0).over(w2).alias("prev_unit_price"),
    )
)

scored_order_items = (
    scored
    .select(
        "*",
        F.first("running_min_quantity", ignorenulls=True).over(w3).alias("first_known_running_min_quantity"),
        F.first("running_min_quantity", ignorenulls=True).over(w4).alias("first_known_running_min_quantity_2"),
        F.avg("prev_unit_price").over(w5).alias("prev_unit_price_avg_6_prior"),
    )
)

base = (
    scored_order_items
    .filter(F.col("prev_unit_price_avg_6_prior").between(123, 284))
    .withColumn("prev_unit_price_per_running_sum_quantity", F.round((F.col("prev_unit_price") / F.col("running_sum_quantity")), 2))
)

matched_keys = (
    base
    .select("id")
    .exceptAll(
        df_order_items
        .filter(F.col("quantity").isNull())
        .select("id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "quantity",
        "running_min_quantity",
        "prev_unit_price",
        "first_known_running_min_quantity_2",
        "prev_unit_price_avg_6_prior",
        "prev_unit_price_per_running_sum_quantity",
    )
    .orderBy(
        F.col("running_min_quantity").desc_nulls_first(),
        "prev_unit_price_avg_6_prior",
    )
)
