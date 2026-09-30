from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("unit_price_whole", "id")
w3 = Window.partitionBy("order_id", "product_id").orderBy(F.col("unit_price").desc(), "id")

cleaned = (
    df_order_items
    .filter(F.col("order_id").isin([2, 40]))
    .select(
        "*",
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
        F.col("quantity").cast("string").alias("quantity_num"),
    )
)

valid = (
    cleaned
    .filter(F.col("product_id").isNotNull())
    .withColumn("unit_price_whole", F.floor("unit_price"))
    .withColumn("unit_price_adjusted", F.round((F.col("unit_price") * 1.1), 2))
)

metrics = (
    valid
    .filter(F.col("unit_price_filled").between(38, 75))
    .select(
        "*",
        F.sum("unit_price_adjusted").over(w).alias("cumulative_unit_price_adjusted"),
        F.lead("quantity", 1).over(w2).alias("next_quantity"),
        F.ntile(3).over(w3).alias("unit_price_bucket"),
    )
)

kept_keys = (
    metrics
    .select("order_id", "id")
    .subtract(
        df_order_items
        .filter(F.col("order_id") == 13)
        .select("order_id", "id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("order_id")
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("unit_price_filled", [0.25, 0.5, 0.75]).alias("unit_price_filled_quartiles"),
    )
)
