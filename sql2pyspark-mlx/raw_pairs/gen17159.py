from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("adjusted_quantity").desc_nulls_first(), "id")
w2 = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id")

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.round((F.col("quantity") * 1.1), 2).alias("adjusted_quantity"),
    )
)

cleaned = (
    base
    .filter(F.col("product_id").isNull())
    .withColumn("unit_price_rounded", F.round("unit_price", 1))
)

latest = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    latest
    .withColumn("next_unit_price_rounded", F.lead("unit_price_rounded").over(w2))
    .withColumn("prev_unit_price_rounded", F.lag("unit_price_rounded").over(w2))
    .withColumn("prev_unit_price", F.lag("unit_price").over(w2))
    .withColumn("event_seq", F.row_number().over(w2))
)

result = (
    enriched
    .groupingSets(
        [["product_id", "order_id"], ["product_id"]],
        "product_id",
        "order_id",
    )
    .agg(
        F.percentile_approx("prev_unit_price_rounded", 0.95).alias("prev_unit_price_rounded_p95"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.percentile_approx("unit_price_rounded", 0.95).alias("unit_price_rounded_p95"),
    )
    .filter(F.col("unit_price_rounded_p95") >= 0)
    .orderBy(F.col("distinct_ids").asc_nulls_last())
)
