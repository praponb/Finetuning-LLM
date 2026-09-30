from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("de_events").desc())

filtered = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2023-04-07").cast("date"))
    .select(
        "*",
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_filled"),
    )
)

with_dims = (
    filtered
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id").eqNullSafe(F.col("p.id")), "inner")
    .select(
        "a.id",
        "a.product_name",
        "a.supplier_id",
        "a.brand_filled",
        F.col("p.supplier_name"),
    )
)

kept = (
    with_dims
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.order_id").isin([19, 29])), "left_semi")
)

joined = (
    kept
    .alias("e")
    .join(df_suppliers.alias("s"), F.col("e.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.supplier_name").like("To%"))
    .select("e.*", F.col("s.rating"), F.col("s.country"))
)

remaining_keys = (
    joined
    .select("id")
    .subtract(
        df_products
        .filter(F.col("supplier_id").isin([5, 9]))
        .select("id")
    )
)

kept_rows = (
    joined
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        "country",
        F.when(F.col("rating") >= 4.3, "HIGH").otherwise("LOW").alias("rating_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_rows"),
        F.count_if(F.col("country") == "DE").alias("de_events"),
    )
    .withColumn("de_events_dense_rank", F.dense_rank().over(w))
)
