from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("price")
w2 = Window.partitionBy("supplier_id").orderBy("launch_date", "id")
w3 = Window.partitionBy("category", "supplier_id")
w4 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.date_trunc("YEAR", "launch_date"), "brand").orderBy(F.col("stock_qty").desc(), "id")

enriched = (
    df_products
    .withColumn("price_dense_rank", F.dense_rank().over(w))
    .withColumn("next_cost", F.lead("cost", 1, 0).over(w2))
    .withColumn("category_stddev_samp_stock_qty", F.stddev_samp("stock_qty").over(w3))
    .withColumn("running_price", F.sum("price").over(w4))
    .withColumn("event_seq", F.row_number().over(w5))
)

base = (
    enriched
    .filter(F.col("launch_date") >= F.lit("2023-08-31").cast("date"))
    .select(
        "id",
        "category",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        "next_cost",
        "running_price",
        F.col("category_stddev_samp_stock_qty").cast("double").alias("category_stddev_samp_stock_qty_num"),
    )
)

base_products = (
    base
    .filter(F.col("stock_qty") == 338)
    .select(
        "id",
        "category",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        "running_price",
        "category_stddev_samp_stock_qty_num",
        F.round((F.col("category_stddev_samp_stock_qty_num") * 1.15), 2).alias("category_stddev_samp_stock_qty_num_scaled"),
    )
)

eligible = (
    base_products
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id").isin([6, 33, 37])), "left_anti")
)

result = (
    eligible
    .withColumn("category_stddev_samp_stock_qty_num_scaled_tier", F.when(F.col("category_stddev_samp_stock_qty_num_scaled") >= 68, "LARGE").otherwise("SMALL"))
    .rollup("supplier_id", "category_stddev_samp_stock_qty_num_scaled_tier")
    .agg(
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.corr("price", "stock_qty").alias("corr_price_stock_qty"),
        F.grouping_id().alias("gid"),
    )
    .orderBy("corr_price_stock_qty")
)
