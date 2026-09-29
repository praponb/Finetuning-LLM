from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")
w2 = Window.partitionBy(F.date_trunc("YEAR", "launch_date_plus_46d"), "supplier_id")

filtered = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (F.col("brand").eqNullSafe(F.lit("Umbrella"))))
    .select(
        "id",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.initcap(F.trim("product_name")).alias("product_name_title"),
    )
)

base = (
    filtered
    .filter(F.col("launch_date") > F.lit("2023-09-23").cast("date"))
    .select(
        "id",
        "category",
        "price",
        "cost",
        "supplier_id",
        F.date_add("launch_date", 46).alias("launch_date_plus_46d"),
        (F.col("price") / F.nullif("stock_qty", F.lit(0))).alias("price_per_stock_qty"),
    )
)

not_flagged = (
    base
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") > "2024-09-01"), "left_semi")
)

metrics = (
    not_flagged
    .withColumn("price_share", F.round((F.col("price") / F.sum("price").over(w)), 4))
    .withColumn("launch_date_plus_46d_period_max_price", F.max("price").over(w2))
    .withColumn("category_avg_price_per_stock_qty", F.avg("price_per_stock_qty").over(w))
)

result = (
    metrics
    .select(
        "id",
        "category",
        "price",
        "cost",
        "supplier_id",
        "launch_date_plus_46d",
        "price_per_stock_qty",
        "price_share",
        "category_avg_price_per_stock_qty",
    )
)
