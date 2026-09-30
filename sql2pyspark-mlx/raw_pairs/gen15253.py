from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("supplier_id", "category").orderBy("stock_qty_plus_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("supplier_id").orderBy("price", "id")
w3 = Window.partitionBy("supplier_id").orderBy("running_max_stock_qty", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("supplier_id").orderBy(F.col("running_min_stock_qty").desc(), "id")
w5 = Window.orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("supplier_id").orderBy("running_min_stock_qty", "id").rowsBetween(-2, -1)
w7 = Window.orderBy("prev_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("supplier_id").orderBy(F.col("seq_no").desc_nulls_last(), "id")
w9 = Window.partitionBy("supplier_id").orderBy("last_known_running_min_stock_qty", "id").rowsBetween(-2, Window.currentRow)
w10 = Window.partitionBy("supplier_id").orderBy("last_known_running_min_stock_qty", "id")
cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.max("cost").alias("max_product_cost"))

valid = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (F.col("launch_date") >= F.lit("2023-05-23").cast("date")))
    .withColumn("brand_group", F.when(F.col("brand").isin(["Initech", "Stark"]), "Group A").otherwise("Other"))
    .withColumn("stock_qty_plus_cost", (F.col("stock_qty") + F.col("cost")))
    .withColumn("brand_clean", F.coalesce("brand", F.lit("DIRECT")))
)

with_windows = (
    valid
    .filter(F.col("supplier_id").isin([23, 28, 33]))
    .select(
        "id",
        "product_name",
        "price",
        "supplier_id",
        "brand_clean",
        F.min("stock_qty").over(w_grp).alias("running_min_stock_qty"),
        F.avg("stock_qty").over(w_grp).alias("running_avg_stock_qty"),
        F.max("stock_qty").over(w_grp).alias("running_max_stock_qty"),
        F.lag("price", 1).over(w2).alias("prev_price"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "price",
        "supplier_id",
        "brand_clean",
        F.avg("price").over(w3).alias("price_avg_3"),
        F.row_number().over(w4).alias("seq_no"),
        F.first("price", ignorenulls=True).over(w5).alias("first_known_price"),
        F.avg("running_min_stock_qty").over(w6).alias("running_min_stock_qty_avg_2_prior"),
        F.last("running_min_stock_qty", ignorenulls=True).over(w7).alias("last_known_running_min_stock_qty"),
    )
)

latest = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w8))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    latest
    .select(
        "id",
        "supplier_id",
        "brand_clean",
        "price_avg_3",
        "seq_no",
        "last_known_running_min_stock_qty",
        F.avg("price_avg_3").over(w9).alias("last3_avg_price_avg_3"),
        F.sum("price_avg_3").over(w9).alias("last3_sum_price_avg_3"),
        F.max("price_avg_3").over(w9).alias("last3_max_price_avg_3"),
        F.min("price_avg_3").over(w9).alias("last3_min_price_avg_3"),
        F.lead("last_known_running_min_stock_qty", 2).over(w10).alias("next_last_known_running_min_stock_qty"),
    )
)

result = (
    metrics
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "supplier_id",
        "price_avg_3",
        "last_known_running_min_stock_qty",
        "last3_min_price_avg_3",
        F.col("max_product_cost"),
    )
    .orderBy(F.col("last3_min_price_avg_3").desc())
)
