from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("latest_rank")
w_recent = Window.partitionBy("product_id", "brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-02-12").cast("date"))
    .select(
        "id",
        "product_id",
        "last_restock",
        F.when(F.col("quantity") >= 249, "High").when(F.col("quantity") >= 77, "Medium").otherwise("Low").alias("quantity_tier"),
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

latest = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

latest_inventory = (
    latest
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
    .select("id", "product_id", "quantity_tier")
)

joined = (
    latest_inventory
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.id", "src.product_id", F.col("p.price"), F.col("p.brand"))
)

with_windows = (
    joined
    .select(
        "id",
        "price",
        F.sum("price").over(w_recent).alias("cumulative_price"),
        F.first("brand", ignorenulls=True).over(w4).alias("first_known_brand"),
        F.last("price", ignorenulls=True).over(w_recent).alias("last_known_price"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("price") >= 1220, "HIGH").otherwise("LOW").alias("price_tier"),
    )
    .agg(
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
        F.count("*").alias("num_rows"),
        F.min("price").alias("min_price"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows_2"),
        F.sum("cumulative_price").alias("total_cumulative_price"),
    )
    .filter(F.col("num_rows_2") >= 1)
)
