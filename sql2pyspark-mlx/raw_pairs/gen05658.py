from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("price").desc_nulls_first(), "id")
w2 = Window.partitionBy("brand").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("brand").orderBy("cost", "id")
w4 = Window.partitionBy("brand")

valid = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-07-23").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        F.round((F.col("stock_qty") / F.col("price")), 2).alias("stock_qty_per_price"),
        F.coalesce("brand", F.lit("DIRECT")).alias("brand_clean"),
    )
)

ranked = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

not_flagged = (
    ranked
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id") == 13), "left_semi")
)

enriched = (
    not_flagged
    .select(
        "*",
        F.sum("stock_qty").over(w2).alias("cumulative_stock_qty"),
        F.lag("stock_qty", 1).over(w3).alias("prev_stock_qty"),
        F.round((F.col("cost") / F.sum("cost").over(w4)), 4).alias("cost_share"),
    )
)

result = (
    enriched
    .withColumn("brand_stddev_samp_cost", F.stddev_samp("cost").over(w4))
    .withColumn("brand_count_price", F.count("price").over(w4))
)
