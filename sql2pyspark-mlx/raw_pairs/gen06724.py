from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("brand", "category").orderBy("cost", "id")
w2 = Window.partitionBy("category", "brand")
w3 = Window.orderBy("launch_date", "id")
w_ordered = Window.partitionBy("category", "supplier_id").orderBy("cost", "id")
w5 = Window.partitionBy("category").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w7 = Window.partitionBy(F.date_trunc("week", "inventory_last_restock"), "category").orderBy("cost", "id")
w8 = Window.partitionBy("category").orderBy(F.col("inventory_last_restock").desc(), "id")

cleaned = (
    df_products
    .filter(F.col("launch_date") > F.to_date(F.lit("2023-03-05")))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.date_sub("launch_date", 22).alias("launch_date_minus_22d"),
    )
)

scored = (
    cleaned
    .filter(F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "launch_date_minus_22d",
        F.lag("stock_qty", 1, 0).over(w_hist).alias("prev_stock_qty"),
        F.avg("cost").over(w2).alias("category_avg_cost"),
        F.lag("price", 2).over(w3).alias("prev_price"),
        F.ntile(3).over(w_ordered).alias("cost_bucket"),
        F.sum("price").over(w5).alias("cumulative_price"),
    )
)

latest_match = (
    scored
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.cost",
        "s.launch_date",
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

scored_products = (
    latest_match
    .withColumn("running_avg_cost", F.avg("cost").over(w5))
    .withColumn("running_count_cost", F.count("cost").over(w5))
    .withColumn("running_min_cost", F.min("cost").over(w5))
    .withColumn("cost_bucket", F.ntile(4).over(w7))
)

latest = (
    scored_products
    .withColumn("dedup_rank", F.row_number().over(w8))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "category",
        "cost",
        "launch_date",
        "inventory_last_restock",
        "running_avg_cost",
        "running_count_cost",
        "running_min_cost",
        "cost_bucket",
        F.when(F.col("category").rlike("^(Home|Books)$"), "GROUP_A").when(F.col("category").rlike("^(Electronics|Sports)$"), "PRIORITY").otherwise("EXTENDED").alias("category_zone"),
    )
    .orderBy(F.col("inventory_last_restock").desc_nulls_first())
)
