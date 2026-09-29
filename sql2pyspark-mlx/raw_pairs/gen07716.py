from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("activity_month").orderBy(F.col("max_sqft").desc())

with_dims = (
    df_inventory
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.brand"))
)

cleaned = (
    with_dims
    .filter((F.col("last_restock") > F.lit("2023-03-12").cast("date")) & F.col("brand").isNotNull())
    .select("*", F.length("brand").alias("brand_length"))
)

cleaned_inventory = (
    cleaned
    .filter(F.col("brand").isNotNull() & (F.col("brand_length") < 53))
    .withColumn("brand_upper", F.upper("brand"))
    .withColumn("resolved_brand", F.coalesce("brand", F.lit("N/A")))
)

enriched = (
    cleaned_inventory
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "inner")
    .select("src.*", F.col("s.store_name"), F.col("s.sqft"), F.col("s.region"))
)

enriched_inventory = (
    enriched
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.price"), F.col("p.category"))
)

result = (
    enriched_inventory
    .groupBy(F.date_trunc("month", "last_restock").alias("activity_month"), "brand")
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.max("sqft").alias("max_sqft"),
        F.count("*").alias("num_rows"),
        F.round(F.sum("price") / F.nullif(F.count(F.when(F.col("category") == "Electronics", 1)), F.lit(0)), 2).alias("price_per_electronics"),
    )
    .withColumn("max_sqft_dense_rank", F.dense_rank().over(w))
    .orderBy(F.col("num_rows").desc_nulls_first())
)
