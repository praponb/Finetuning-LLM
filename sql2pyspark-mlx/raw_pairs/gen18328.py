from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("category").orderBy("price_minus_stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.substring("brand", 1, 2).alias("brand_prefix"),
        (F.col("price") - F.col("stock_qty")).alias("price_minus_stock_qty"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("resolved_brand"),
    )
)

enriched = (
    filtered
    .select(
        "id",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "brand_prefix",
        "price_minus_stock_qty",
        F.min("price").over(w_hist).alias("running_min_price"),
        F.count("price").over(w_hist).alias("running_count_price"),
        F.max("price").over(w_hist).alias("running_max_price"),
        F.avg("price").over(w_hist).alias("running_avg_price"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.last_restock") >= F.col("s.launch_date")), "left_anti")
)

valid = (
    kept
    .filter(F.col("stock_qty") <= 138)
    .select(
        "id",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "brand_prefix",
        "price_minus_stock_qty",
        "running_count_price",
        "running_max_price",
        F.last_day("launch_date").alias("launch_date_month_end"),
    )
)

kept_products = (
    valid
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.units") < 24), "left_anti")
)

result = (
    kept_products
    .withColumn("year_start", F.date_trunc("YEAR", "launch_date_month_end"))
    .withColumn("activity_month", F.date_trunc("MONTH", "launch_date_month_end"))
    .groupingSets(
        [["year_start", "activity_month"], ["year_start"], []],
        "year_start",
        "activity_month",
    )
    .agg(
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.percentile_approx("running_count_price", [0.25, 0.5, 0.75]).alias("running_count_price_quartiles"),
        F.max("price").alias("max_price"),
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.sum(F.when(F.col("brand") == "Globex", F.col("stock_qty")).otherwise(0)).alias("globex_stock_qty"),
    )
    .orderBy(F.col("activity_month").desc_nulls_first())
)
