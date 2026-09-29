from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("stock_qty").desc_nulls_last())
w3 = Window.partitionBy("category", "brand").orderBy("running_price", "id")
w4 = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc())
w5 = Window.orderBy("cost", "id")
w6 = Window.partitionBy("supplier_id").orderBy("cost_cume_dist", "id")
w7 = Window.partitionBy("category").orderBy("running_price_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy(F.col("prev_stock_qty_dense_rank").desc_nulls_first())
w10 = Window.partitionBy("supplier_id")

valid = (
    df_products
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-11-05"))) & (F.year("launch_date") == 2023))
    .select("*", F.lower("brand").alias("brand_lower"))
)

with_windows = (
    valid
    .withColumn("first_known_cost", F.first("cost", ignorenulls=True).over(w_part))
    .withColumn("running_price", F.sum("price").over(w_part))
    .withColumn("stock_qty_dense_rank", F.dense_rank().over(w2))
)

enriched = (
    with_windows
    .select(
        "*",
        F.ntile(10).over(w3).alias("running_price_bucket"),
        F.cume_dist().over(w4).alias("cost_cume_dist"),
        F.lead("cost", 1).over(w5).alias("next_cost"),
    )
)

base = (
    enriched
    .filter(F.col("launch_date") > F.lit("2023-12-08").cast("date"))
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        "brand_lower",
        "running_price",
        "stock_qty_dense_rank",
        "running_price_bucket",
        "cost_cume_dist",
        "next_cost",
        F.array_contains("tags", "bestseller").alias("is_bestseller"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_filled"),
    )
)

scored = (
    base
    .withColumn("prev_stock_qty_dense_rank", F.lag("stock_qty_dense_rank", 1).over(w6))
    .withColumn("cumulative_price", F.sum("price").over(w_part))
    .withColumn("running_next_cost", F.sum("next_cost").over(w7))
)

valid_products = (
    scored
    .filter(F.col("supplier_id").isNotNull() & (F.col("running_next_cost") <= 1556))
    .select(
        "*",
        F.col("stock_qty_dense_rank").cast("string").alias("stock_qty_dense_rank_str"),
    )
)

with_windows_products = (
    valid_products
    .select(
        "*",
        F.sum("stock_qty").over(w8).alias("running_stock_qty"),
        F.percent_rank().over(w9).alias("prev_stock_qty_dense_rank_percent_rank"),
        F.sum("stock_qty_dense_rank").over(w10).alias("supplier_id_sum_stock_qty_dense_rank"),
    )
)

result = (
    with_windows_products
    .groupBy("supplier_id")
    .pivot("category", ["Electronics", "Fashion", "Home"])
    .agg(F.avg("cost"))
    .select(
        "supplier_id",
        F.col("Electronics").alias("electronics"),
        F.col("Fashion").alias("fashion"),
        F.col("Home").alias("home"),
    )
)
