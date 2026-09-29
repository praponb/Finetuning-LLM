from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)

lines = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

lines_carts = (
    lines
    .select(
        "id",
        "created_at",
        "qty",
        "promo",
        F.explode("coupon_codes").alias("element"),
    )
)

filtered = (
    lines_carts
    .filter((F.col("created_at") >= F.lit("2025-03-01 20:00:00").cast("timestamp")) & F.col("element").isNotNull())
    .select(
        "*",
        F.initcap(F.trim("element")).alias("element_title"),
        F.coalesce("element", F.lit("OTHER")).alias("element_filled"),
    )
)

metrics = (
    filtered
    .filter(F.col("created_at") < "2025-02-20")
    .select("id", "qty", "promo", F.sum("qty").over(w).alias("qty_sum_30m"))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("qty_sum_30m") >= 24, "HIGH").otherwise("LOW").alias("qty_sum_30m_tier"),
        F.when(F.col("qty") >= 4, "LARGE").otherwise("SMALL").alias("qty_tier"),
    )
    .agg(
        F.count_if(F.col("promo") == "BUNDLE").alias("bundle_events"),
        F.round(F.avg("qty"), 2).alias("avg_qty"),
    )
    .orderBy(F.col("avg_qty").desc())
)
