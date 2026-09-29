from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("brand")
w4 = Window.partitionBy("category").orderBy("running_sum_price", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy("brand").orderBy("running_sum_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("brand", "product_id").orderBy("running_sum_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("running_count_price", "id").rowsBetween(-5, -1)
w8 = Window.partitionBy("category").orderBy("cumulative_price_avg_5", "id").rowsBetween(-5, -1)
w9 = Window.partitionBy("category").orderBy("running_sum_price", "id")
w10 = Window.partitionBy("category").orderBy("cumulative_price_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.sum("quantity").alias("sum_inventory_quantity"))

enriched = (
    df_order_items
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.price"), F.col("p.category"), F.col("p.brand"))
)

metrics = (
    enriched
    .filter(F.col("product_id") == 40)
    .select(
        "id",
        "product_id",
        "category",
        "brand",
        F.min("price").over(w).alias("running_min_price"),
        F.count("price").over(w).alias("running_count_price"),
        F.sum("price").over(w).alias("running_sum_price"),
        F.avg("price").over(w).alias("running_avg_price"),
        F.avg("price").over(w2).alias("price_avg_5"),
    )
)

enriched_order_items = (
    metrics
    .select(
        "id",
        "product_id",
        "category",
        "running_min_price",
        "running_sum_price",
        F.sum("price_avg_5").over(w3).alias("brand_sum_price_avg_5"),
        F.avg("running_avg_price").over(w4).alias("running_avg_price_avg_2_prior"),
        F.sum("running_avg_price").over(w5).alias("running_running_avg_price"),
        F.sum("price_avg_5").over(w6).alias("cumulative_price_avg_5"),
        F.avg("running_sum_price").over(w7).alias("running_sum_price_avg_5_prior"),
    )
)

scored = (
    enriched_order_items
    .select(
        "*",
        F.avg("running_running_avg_price").over(w8).alias("running_running_avg_price_avg_5_prior"),
        F.ntile(10).over(w9).alias("running_sum_price_bucket"),
        F.first("cumulative_price_avg_5", ignorenulls=True).over(w10).alias("first_known_cumulative_price_avg_5"),
        F.lead("cumulative_price_avg_5", 1).over(w9).alias("next_cumulative_price_avg_5"),
    )
)

result = (
    scored
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "running_min_price",
        "running_sum_price",
        "brand_sum_price_avg_5",
        "running_avg_price_avg_2_prior",
        "running_running_avg_price",
        "cumulative_price_avg_5",
        "running_running_avg_price_avg_5_prior",
        F.col("sum_inventory_quantity"),
    )
)
