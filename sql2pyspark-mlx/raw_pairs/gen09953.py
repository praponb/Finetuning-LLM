from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("category").orderBy("price", "id")
w4 = Window.orderBy(F.col("cost").desc(), "id")
w5 = Window.partitionBy("category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w_recent = Window.partitionBy("category").orderBy("last_restock", "id").rowsBetween(-6, Window.currentRow)
w9 = Window.orderBy(F.col("price_bucket").asc_nulls_last())
w10 = Window.orderBy("cost_bucket", "id")
w11 = Window.orderBy("quantity", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_products
    .filter((~F.col("brand").eqNullSafe(F.lit("Stark"))) & (F.col("stock_qty") != 325))
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        F.coalesce("brand", F.lit("unassigned")).alias("brand_filled"),
        F.substring("category", 1, 2).alias("category_prefix"),
        F.coalesce("brand", F.lit("OTHER")).alias("resolved_brand"),
    )
)

with_windows = (
    base
    .filter(F.col("price").between(350, 850))
    .select(
        "id",
        "category",
        "brand_filled",
        F.sum("cost").over(w).alias("running_cost"),
        F.sum("cost").over(w2).alias("cumulative_cost"),
        F.ntile(3).over(w3).alias("price_bucket"),
        F.ntile(5).over(w4).alias("cost_bucket"),
        F.first("cost", ignorenulls=True).over(w5).alias("first_known_cost"),
    )
)

latest_match = (
    with_windows
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.cumulative_cost",
        "s.price_bucket",
        "s.cost_bucket",
        F.col("c.last_restock"),
    )
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity"),
    )
)

with_windows_products = (
    with_latest
    .select(
        "id",
        "price_bucket",
        "cost_bucket",
        "last_restock",
        "inventory_last_restock",
        "quantity",
        F.min("cost_bucket").over(w_recent).alias("last7_min_cost_bucket"),
        F.avg("cost_bucket").over(w_recent).alias("last7_avg_cost_bucket"),
        F.count("cost_bucket").over(w_recent).alias("last7_count_cost_bucket"),
    )
)

with_windows_products_2 = (
    with_windows_products
    .select(
        "id",
        "inventory_last_restock",
        "quantity",
        "last7_min_cost_bucket",
        F.rank().over(w9).alias("price_bucket_rank"),
        F.row_number().over(w10).alias("seq_no"),
        F.avg("quantity").over(w11).alias("quantity_avg_6"),
    )
)

result = (
    with_windows_products_2
    .groupBy("seq_no")
    .agg(
        F.round(F.avg("quantity_avg_6"), 2).alias("avg_quantity_avg_6"),
        F.round(F.avg("last7_min_cost_bucket"), 2).alias("avg_last7_min_cost_bucket"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("distinct_ids") < 1000)
    .orderBy(F.col("avg_quantity_avg_6").asc_nulls_last())
)
