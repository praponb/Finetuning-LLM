from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id")

filtered = (
    df_order_items
    .filter(F.col("quantity") >= 5)
    .select("*", F.round((F.col("quantity") * 0.85), 2).alias("quantity_scaled"))
)

latest = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

remaining_keys = (
    latest
    .select("product_id")
    .intersect(
        df_order_items
        .filter(F.col("product_id").isNull())
        .select("product_id")
    )
)

verified = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("product_id").isNotNull() & (F.col("unit_price") < 270))
    .select(
        "id",
        "product_id",
        "quantity",
        "quantity_scaled",
        F.col("quantity").cast("string").alias("quantity_num"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("quantity_scaled") >= 73, "LARGE").otherwise("SMALL").alias("quantity_scaled_tier"),
        F.when(F.col("quantity") >= 7, "LARGE").otherwise("SMALL").alias("quantity_tier"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("*").alias("row_count"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
    )
    .orderBy("quantity_tier")
)
