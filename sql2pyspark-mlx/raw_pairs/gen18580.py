from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "supplier_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("category").orderBy("least_stock_qty_price")
w3 = Window.partitionBy("category")
w4 = Window.orderBy("launch_date", "id")
w5 = Window.orderBy(F.col("cost").desc())
w6 = Window.partitionBy(F.date_trunc("month", "launch_date")).orderBy("last_known_least_stock_qty_price", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.orderBy(F.col("prev_cost").asc_nulls_last())

base = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "*",
        F.least("stock_qty", "price").alias("least_stock_qty_price"),
        F.length("brand").alias("brand_length"),
    )
)

valid = (
    base
    .filter((F.col("launch_date") > F.lit("2023-06-19").cast("date")) & F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        "least_stock_qty_price",
        F.upper("category").alias("category_upper"),
        F.array_contains("tags", "bestseller").alias("is_bestseller"),
    )
)

scored = (
    valid
    .select(
        "id",
        "launch_date",
        "tags",
        F.last("least_stock_qty_price", ignorenulls=True).over(w).alias("last_known_least_stock_qty_price"),
        F.cume_dist().over(w2).alias("least_stock_qty_price_cume_dist"),
        F.stddev_samp("cost").over(w3).alias("category_stddev_samp_cost"),
        F.lag("cost", 1).over(w4).alias("prev_cost"),
        F.dense_rank().over(w5).alias("cost_dense_rank"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "launch_date",
        "least_stock_qty_price_cume_dist",
        "cost_dense_rank",
        F.avg("last_known_least_stock_qty_price").over(w6).alias("last_known_least_stock_qty_price_avg_3"),
        F.cume_dist().over(w7).alias("prev_cost_cume_dist"),
    )
)

result = (
    enriched
    .select(
        "id",
        "launch_date",
        "least_stock_qty_price_cume_dist",
        "cost_dense_rank",
        "last_known_least_stock_qty_price_avg_3",
        "prev_cost_cume_dist",
    )
)
