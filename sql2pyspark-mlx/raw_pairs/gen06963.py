from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc(), "id")
w2 = Window.orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("supplier_id", "category").orderBy(F.col("stock_qty").desc_nulls_first())
w4 = Window.orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("category").orderBy(F.col("cumulative_stock_qty").desc(), "id")
w6 = Window.partitionBy("category").orderBy(F.col("stock_qty_cume_dist").desc(), "id")
w7 = Window.partitionBy("category").orderBy("running_cost", "id").rowsBetween(-5, -1)
w8 = Window.partitionBy("category")

latest = (
    df_products
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

scored = (
    latest
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "tags",
        F.sum("stock_qty").over(w2).alias("cumulative_stock_qty"),
        F.cume_dist().over(w3).alias("stock_qty_cume_dist"),
        F.sum("cost").over(w4).alias("running_cost"),
    )
)

base = (
    scored
    .filter(F.col("product_name").isNotNull())
    .select(
        "*",
        F.concat("category", F.lit(", "), "product_name").alias("category_product_name"),
        F.array_contains("tags", "bestseller").alias("is_bestseller"),
    )
)

valid = (
    base
    .filter(F.col("running_cost") >= 2984)
    .withColumn("cumulative_stock_qty_plus_cost", (F.col("cumulative_stock_qty") + F.col("cost")))
)

eligible = (
    valid
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.order_id").isin([3, 4, 14, 25])), "left_anti")
)

latest_products = (
    eligible
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    latest_products
    .filter(F.col("cumulative_stock_qty_plus_cost") > 58)
    .select(
        "*",
        F.row_number().over(w6).alias("row_seq"),
        F.avg("price").over(w7).alias("price_avg_5_prior"),
        F.count("cumulative_stock_qty_plus_cost").over(w8).alias("category_count_cumulative_stock_qty_plus_cost"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "cumulative_stock_qty",
        "stock_qty_cume_dist",
        "running_cost",
        "category_product_name",
    )
    .orderBy(F.col("cumulative_stock_qty").asc_nulls_first())
)
