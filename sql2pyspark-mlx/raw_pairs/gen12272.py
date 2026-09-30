from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy(F.col("quantity_rounded").desc_nulls_first(), "id")
w4 = Window.partitionBy("product_id").orderBy(F.col("quantity_rounded").asc_nulls_first(), "id")
w_grp = Window.partitionBy("product_id").orderBy("launch_date", "id")

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.round("quantity", 1).alias("quantity_rounded"),
    )
)

cleaned = (
    base
    .filter(F.col("order_id") == 22)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "quantity_rounded",
        F.least("quantity_rounded", "quantity").alias("least_quantity_rounded_quantity"),
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "product_id",
        "quantity_rounded",
        F.avg("least_quantity_rounded_quantity").over(w_hist).alias("least_quantity_rounded_quantity_avg_2"),
        F.avg("unit_price").over(w2).alias("unit_price_avg_3"),
    )
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

ranked = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "product_id",
        "least_quantity_rounded_quantity_avg_2",
        "unit_price_avg_3",
    )
)

with_dims = (
    ranked
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "left")
    .filter(F.size(F.col("b.tags")) > 2)
    .select(
        "e.id",
        "e.product_id",
        "e.least_quantity_rounded_quantity_avg_2",
        "e.unit_price_avg_3",
        F.col("b.launch_date"),
    )
)

enriched_order_items = (
    with_dims
    .select(
        "id",
        "product_id",
        "least_quantity_rounded_quantity_avg_2",
        "unit_price_avg_3",
        F.lag("least_quantity_rounded_quantity_avg_2").over(w_grp).alias("prev_least_quantity_rounded_quantity_avg_2"),
        F.lead("least_quantity_rounded_quantity_avg_2").over(w_grp).alias("next_least_quantity_rounded_quantity_avg_2"),
        F.lag("unit_price_avg_3").over(w_grp).alias("prev_unit_price_avg_3"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)

summary = (
    enriched_order_items
    .rollup(
        "product_id",
        F.when(F.col("event_seq") >= 9, "PREMIUM").otherwise("STANDARD").alias("event_seq_tier"),
    )
    .agg(
        F.max("prev_least_quantity_rounded_quantity_avg_2").alias("max_prev_least_quantity_rounded_quantity_avg_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("prev_unit_price_avg_3", [0.25, 0.5, 0.75]).alias("prev_unit_price_avg_3_quartiles"),
        F.max("least_quantity_rounded_quantity_avg_2").alias("max_least_quantity_rounded_quantity_avg_2"),
    )
)

result = (
    summary
    .select("event_seq_tier", "max_least_quantity_rounded_quantity_avg_2")
    .orderBy(F.col("event_seq_tier").desc())
)
