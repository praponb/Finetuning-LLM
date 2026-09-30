from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "supplier_id").orderBy("cost", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy(F.month("launch_date"), "brand").orderBy("cost", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc())
w4 = Window.partitionBy(F.month("launch_date"), "brand").orderBy("price_whole", "id")
w5 = Window.partitionBy("brand", "category").orderBy("price_whole_avg_5_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("price_whole_avg_5", "id")
w7 = Window.partitionBy("supplier_id").orderBy(F.col("price").desc(), "id")
w8 = Window.partitionBy("brand").orderBy("price_whole_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("category").orderBy("price_whole_avg_5", "id").rowsBetween(-6, -1)
w10 = Window.partitionBy("supplier_id", "brand")
w11 = Window.partitionBy("category").orderBy("price_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("brand", "category").orderBy(F.col("price_whole_avg_5").desc_nulls_first())

filtered = (
    df_products
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-03-14"))) & (F.col("brand") == "Initech"))
    .select(
        "*",
        F.ceil("price").alias("price_whole"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_filled"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.avg("price_whole").over(w).alias("price_whole_avg_5"),
        F.avg("price_whole").over(w2).alias("price_whole_avg_5_2"),
        F.percent_rank().over(w3).alias("stock_qty_percent_rank"),
        F.ntile(10).over(w4).alias("price_whole_bucket"),
    )
)

metrics_products = (
    metrics
    .filter(F.month("launch_date").isin([2, 4, 5]))
    .select(
        "id",
        "category",
        "brand",
        "cost",
        "supplier_id",
        "price_whole",
        "price_whole_avg_5",
        "price_whole_avg_5_2",
        F.last("stock_qty", ignorenulls=True).over(w5).alias("last_known_stock_qty"),
        F.lead("price_whole_avg_5", 1).over(w6).alias("next_price_whole_avg_5"),
        F.ntile(3).over(w7).alias("price_bucket"),
        F.sum("price").over(w8).alias("running_price"),
        F.avg("stock_qty").over(w9).alias("stock_qty_avg_6_prior"),
    )
)

enriched = (
    metrics_products
    .select(
        "id",
        "category",
        "brand",
        "supplier_id",
        "next_price_whole_avg_5",
        "price_bucket",
        "running_price",
        F.stddev_samp("last_known_stock_qty").over(w10).alias("supplier_id_stddev_samp_last_known_stock_qty"),
        F.round((F.col("cost") / F.sum("cost").over(w10)), 4).alias("cost_share"),
        F.sum("price_bucket").over(w11).alias("running_price_bucket"),
        F.cume_dist().over(w12).alias("price_whole_avg_5_cume_dist"),
    )
)

result = (
    enriched
    .alias("s")
    .join(df_suppliers.alias("dim"), F.col("s.supplier_id") == F.col("dim.id"), "inner")
    .cube(F.col("dim.city").alias("city"), F.col("s.category").alias("category"))
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("s.supplier_id_stddev_samp_last_known_stock_qty")).alias("min_supplier_id_stddev_samp_last_known_stock_qty"),
    )
    .orderBy(F.col("min_supplier_id_stddev_samp_last_known_stock_qty").asc_nulls_last())
)
