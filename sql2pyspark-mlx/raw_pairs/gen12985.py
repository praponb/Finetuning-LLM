from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")
w2 = Window.partitionBy("category", "brand").orderBy("price", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("product_id", "brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("category", "product_id").orderBy("quantity", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.orderBy(F.col("price").asc_nulls_last())
w6 = Window.partitionBy("category").orderBy(F.col("cumulative_quantity").desc_nulls_first(), "id")
w7 = Window.orderBy("cumulative_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("category")
w9 = Window.partitionBy("category").orderBy(F.col("cumulative_cumulative_quantity").asc_nulls_last(), "id")

base = (
    df_inventory
    .filter(F.col("quantity") >= 63)
    .withColumn("quantity_str", F.col("quantity").cast("double"))
)

enriched = (
    base
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .filter(F.col("b.supplier_id").isin([18, 37, 39]))
    .select(
        "src.id",
        "src.product_id",
        "src.quantity",
        F.col("b.brand"),
        F.col("b.category"),
        F.col("b.price"),
    )
)

enriched_inventory = (
    enriched
    .select(
        "id",
        "category",
        "price",
        F.stddev_samp("price").over(w).alias("brand_stddev_samp_price"),
        F.avg("quantity").over(w2).alias("quantity_avg_5_prior"),
        F.sum("quantity").over(w3).alias("cumulative_quantity"),
        F.avg("quantity").over(w4).alias("quantity_avg_6"),
        F.dense_rank().over(w5).alias("price_dense_rank"),
    )
)

ranked = (
    enriched_inventory
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched_inventory_2 = (
    ranked
    .select(
        "*",
        F.sum("cumulative_quantity").over(w7).alias("cumulative_cumulative_quantity"),
        F.min("brand_stddev_samp_price").over(w8).alias("category_min_brand_stddev_samp_price"),
    )
)

deduped = (
    enriched_inventory_2
    .withColumn("rank_in_group", F.row_number().over(w9))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .rollup(
        F.when(F.col("cumulative_cumulative_quantity") >= 4629, "LARGE").otherwise("SMALL").alias("cumulative_cumulative_quantity_tier"),
        F.when(F.col("brand_stddev_samp_price") >= 953, "HIGH").otherwise("LOW").alias("brand_stddev_samp_price_tier"),
    )
    .agg(
        F.sum(F.when(F.col("category") == "Sports", F.col("price")).otherwise(0)).alias("sports_price"),
        F.round(F.avg("brand_stddev_samp_price"), 2).alias("avg_brand_stddev_samp_price"),
        F.round(F.sum("price") / F.nullif(F.count(F.when(F.col("category") == "Books", 1)), F.lit(0)), 2).alias("price_per_books"),
        F.percentile_approx("cumulative_quantity", 0.95).alias("cumulative_quantity_p95"),
    )
    .orderBy("cumulative_cumulative_quantity_tier")
)
