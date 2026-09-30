from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id")
w2 = Window.partitionBy("product_id", "order_id").orderBy("line_total_2", "id")
w3 = Window.partitionBy("order_id")
w4 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc_nulls_last())
w5 = Window.partitionBy("product_id")
w6 = Window.orderBy(F.col("prev_quantity").asc_nulls_last())
w7 = Window.partitionBy("order_id").orderBy(F.col("product_id_min_prev_quantity").asc_nulls_first())

cleaned = (
    df_order_items
    .filter(F.col("unit_price").between(200, 350))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("unit_price") + F.col("quantity")).alias("line_total"),
        (F.col("unit_price") - F.col("quantity")).alias("line_total_2"),
    )
)

metrics = (
    cleaned
    .filter(F.col("quantity") != 4)
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "line_total_2",
        F.lead("quantity", 2).over(w).alias("next_quantity"),
        F.lag("quantity", 1).over(w2).alias("prev_quantity"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w3)), 4).alias("unit_price_share"),
    )
)

matched_keys = (
    metrics
    .select("order_id")
    .subtract(
        df_order_items
        .filter(F.col("unit_price") < 400)
        .select("order_id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .select(
        "order_id",
        "line_total_2",
        "next_quantity",
        "prev_quantity",
        F.percent_rank().over(w4).alias("unit_price_percent_rank"),
        F.min("prev_quantity").over(w5).alias("product_id_min_prev_quantity"),
        F.rank().over(w6).alias("prev_quantity_rank"),
    )
)

scored = (
    with_windows
    .withColumn("product_id_min_prev_quantity_cume_dist", F.cume_dist().over(w7))
)

stats = (
    scored
    .groupBy(
        F.when(F.col("prev_quantity_rank") >= 8, "PREMIUM").otherwise("STANDARD").alias("prev_quantity_rank_tier"),
        F.when(F.col("product_id_min_prev_quantity") >= 7, "HIGH").otherwise("LOW").alias("product_id_min_prev_quantity_tier"),
    )
    .agg(
        F.percentile_approx("prev_quantity", 0.5).alias("prev_quantity_p50"),
        F.percentile_approx("line_total_2", [0.25, 0.5, 0.75]).alias("line_total_2_quartiles"),
        F.count("*").alias("row_count"),
        F.avg("prev_quantity_rank").alias("avg_prev_quantity_rank"),
    )
)

result = (
    stats
    .select(
        "prev_quantity_rank_tier",
        "product_id_min_prev_quantity_tier",
        "prev_quantity_p50",
    )
    .orderBy(F.col("prev_quantity_rank_tier").desc_nulls_first())
)
