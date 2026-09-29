from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("launch_date"), "product_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("brand").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.year("launch_date"), "product_id")
w_grp = Window.partitionBy("brand", "category").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("brand").orderBy("price", "id")

cleaned = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4, 5]))
    .select("*", F.year("last_restock").alias("last_restock_year"))
)

joined = (
    cleaned
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.price") >= 670)
    .select(
        "e.id",
        "e.product_id",
        "e.quantity",
        F.col("p.brand"),
        F.col("p.launch_date"),
        F.col("p.product_name"),
    )
)

enriched = (
    joined
    .select(
        "id",
        "product_id",
        "quantity",
        "product_name",
        F.sum("quantity").over(w).alias("cumulative_quantity"),
        F.sum("quantity").over(w2).alias("running_quantity"),
        F.stddev_samp("quantity").over(w3).alias("launch_date_year_stddev_samp_quantity"),
    )
)

joined_inventory = (
    enriched
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.product_id",
        "src.quantity",
        "src.running_quantity",
        F.col("b.price"),
        F.col("b.brand"),
        F.col("b.category"),
    )
)

result = (
    joined_inventory
    .select(
        "id",
        "product_id",
        "quantity",
        "running_quantity",
        "brand",
        F.avg("running_quantity").over(w_grp).alias("running_avg_running_quantity"),
        F.sum("running_quantity").over(w_grp).alias("running_sum_running_quantity"),
        F.count("running_quantity").over(w_grp).alias("running_count_running_quantity"),
        F.lag("running_quantity", 1).over(w5).alias("prev_running_quantity"),
    )
)
