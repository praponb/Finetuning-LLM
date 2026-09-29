from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("price", "id")
w2 = Window.partitionBy(F.month("launch_date"))
w3 = Window.partitionBy("brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("category").orderBy(F.col("corr_cost_price").desc())
w5 = Window.partitionBy("category").orderBy("running_stock_qty", "id")
w6 = Window.partitionBy("category")
w7 = Window.partitionBy("brand").orderBy(F.col("cost").desc())
w8 = Window.partitionBy("category").orderBy("category_avg_stock_qty", "id").rowsBetween(-6, Window.currentRow)
w9 = Window.partitionBy("brand").orderBy("corr_cost_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("brand").orderBy("running_cost", "id")

enriched = (
    df_products
    .select(
        "*",
        F.ntile(5).over(w).alias("price_bucket"),
        F.corr("cost", "price").over(w2).alias("corr_cost_price"),
        F.sum("stock_qty").over(w3).alias("running_stock_qty"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.dense_rank().over(w4).alias("corr_cost_price_dense_rank"),
        F.ntile(3).over(w5).alias("running_stock_qty_bucket"),
        F.avg("stock_qty").over(w6).alias("category_avg_stock_qty"),
    )
)

ranked = (
    metrics
    .withColumn("rn", F.rank().over(w7))
    .filter(F.col("rn") == 1)
)

enriched_products = (
    ranked
    .filter(F.col("running_stock_qty") == 614)
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "price_bucket",
        "running_stock_qty",
        "category_avg_stock_qty",
        F.count("corr_cost_price").over(w8).alias("last7_count_corr_cost_price"),
        F.min("corr_cost_price").over(w8).alias("last7_min_corr_cost_price"),
        F.max("corr_cost_price").over(w8).alias("last7_max_corr_cost_price"),
        F.avg("corr_cost_price").over(w8).alias("last7_avg_corr_cost_price"),
        F.sum("cost").over(w9).alias("running_cost"),
    )
)

scored = (
    enriched_products
    .select(
        "*",
        F.lead("last7_max_corr_cost_price").over(w10).alias("next_last7_max_corr_cost_price"),
        F.lag("last7_max_corr_cost_price").over(w10).alias("prev_last7_max_corr_cost_price"),
        F.lag("price_bucket").over(w10).alias("prev_price_bucket"),
        F.row_number().over(w10).alias("seq_no"),
    )
)

result = (
    scored
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "stock_qty",
        "tags",
        "price_bucket",
        "category_avg_stock_qty",
        "last7_count_corr_cost_price",
        F.when(F.col("product_name").rlike("^(Office Chair|Yoga Mat)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("product_name_zone"),
    )
    .orderBy(F.col("price").desc())
)
