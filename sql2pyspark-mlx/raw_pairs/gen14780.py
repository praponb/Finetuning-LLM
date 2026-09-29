from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("brand", "category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country")
w4 = Window.orderBy("running_sum_stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country").orderBy("is_sg", "id").rowsBetween(-6, -1)
w6 = Window.orderBy("running_avg_stock_qty_avg_6_prior", "id").rowsBetween(-3, Window.currentRow)

joined = (
    df_products
    .alias("s")
    .join(df_suppliers.alias("b"), F.col("s.supplier_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.country"))
)

metrics = (
    joined
    .select(
        "id",
        "product_name",
        "price",
        "launch_date",
        "tags",
        "country",
        F.avg("stock_qty").over(w).alias("running_avg_stock_qty"),
        F.count("stock_qty").over(w).alias("running_count_stock_qty"),
        F.sum("stock_qty").over(w).alias("running_sum_stock_qty"),
        F.first("country", ignorenulls=True).over(w2).alias("first_known_country"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.store_id").isin([3, 5])), "left_anti")
)

filtered = (
    eligible
    .filter(F.col("country").isNotNull() & (F.col("running_sum_stock_qty") < 600))
    .withColumn("product_name_country_key", F.concat_ws("-", "product_name", "country"))
    .withColumn("is_sg", F.when(F.col("country") == "SG", 1).otherwise(0))
    .withColumn("country_clean", F.coalesce("country", F.lit("N/A")))
)

scored = (
    filtered
    .filter(F.month("launch_date").isin([3, 5, 12]))
    .select(
        "id",
        "running_count_stock_qty",
        "running_sum_stock_qty",
        "first_known_country",
        "is_sg",
        "country_clean",
        F.sum("running_sum_stock_qty").over(w3).alias("country_sum_running_sum_stock_qty"),
        F.sum("running_sum_stock_qty").over(w4).alias("running_running_sum_stock_qty"),
        F.avg("running_avg_stock_qty").over(w5).alias("running_avg_stock_qty_avg_6_prior"),
    )
)

result = (
    scored
    .select(
        "id",
        "running_count_stock_qty",
        "first_known_country",
        "is_sg",
        "country_clean",
        "country_sum_running_sum_stock_qty",
        "running_running_sum_stock_qty",
        "running_avg_stock_qty_avg_6_prior",
        F.sum("country_sum_running_sum_stock_qty").over(w6).alias("last4_sum_country_sum_running_sum_stock_qty"),
        F.avg("country_sum_running_sum_stock_qty").over(w6).alias("last4_avg_country_sum_running_sum_stock_qty"),
        F.max("country_sum_running_sum_stock_qty").over(w6).alias("last4_max_country_sum_running_sum_stock_qty"),
        F.min("country_sum_running_sum_stock_qty").over(w6).alias("last4_min_country_sum_running_sum_stock_qty"),
    )
)
