from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("rating").desc(), "id")

valid = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-08-26").cast("date")) & F.col("brand").isNotNull())
    .select(
        "*",
        F.array_join("tags", "|").alias("tags_text"),
        F.ceil("stock_qty").alias("stock_qty_whole"),
    )
)

enriched = (
    valid
    .alias("e")
    .join(df_suppliers.alias("d"), F.col("e.supplier_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "e.id",
        "e.category",
        "e.brand",
        "e.price",
        "e.cost",
        "e.stock_qty",
        "e.tags",
        "e.supplier_id",
        "e.tags_text",
        F.col("d.rating"),
        F.col("d.supplier_name"),
        F.col("d.city"),
    )
)

enriched_products = (
    enriched
    .select(
        "*",
        F.avg("stock_qty").over(w).alias("running_avg_stock_qty"),
        F.max("stock_qty").over(w).alias("running_max_stock_qty"),
        F.count("stock_qty").over(w).alias("running_count_stock_qty"),
        F.ntile(4).over(w2).alias("rating_bucket"),
    )
)

matched_keys = (
    enriched_products
    .select("brand", "id")
    .subtract(
        df_products
        .filter(F.size("tags") > 1)
        .select("brand", "id")
    )
)

verified = (
    enriched_products
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("brand").isNotNull() & (~F.col("brand").eqNullSafe(F.lit("Umbrella"))))
    .withColumn("category_length", F.length("category"))
    .withColumn("city_clean", F.coalesce("city", F.lit("OTHER")))
)

result = (
    filtered
    .alias("src")
    .join(df_suppliers.alias("s"), F.col("src.supplier_id") == F.col("s.id"), "left")
    .groupBy(
        F.col("s.country").alias("country"),
        F.col("src.category").alias("category"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("src.price")).alias("total_price"),
        F.max(F.col("src.cost")).alias("max_cost"),
    )
)
