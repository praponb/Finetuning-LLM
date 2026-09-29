from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc_nulls_last())
w3 = Window.partitionBy(F.month("launch_date"), "category").orderBy("stock_qty", "id")
w4 = Window.partitionBy("category", "brand").orderBy(F.col("revenue").desc_nulls_first())
w5 = Window.orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w7 = Window.partitionBy("category").orderBy("stock_qty", "id")
w8 = Window.partitionBy("category").orderBy(F.col("inventory_quantity").desc(), "id")
w9 = Window.partitionBy("category").orderBy("revenue", "id")
w10 = Window.partitionBy("category").orderBy("prev_next_price", "id")
w11 = Window.partitionBy("category").orderBy("inventory_quantity", "id")
w12 = Window.partitionBy("category").orderBy("prev_cost", "id").rowsBetween(-2, -1)

latest_match = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date"), F.col("c.revenue"))
)

scored = (
    latest_match
    .select(
        "id",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "supplier_id",
        "revenue",
        F.rank().over(w2).alias("cost_rank"),
        F.lead("price", 1).over(w3).alias("next_price"),
        F.cume_dist().over(w4).alias("revenue_cume_dist"),
        F.sum("price").over(w5).alias("cumulative_price"),
    )
)

latest_match_products = (
    scored
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.cost",
        "s.stock_qty",
        "s.revenue",
        "s.cost_rank",
        "s.next_price",
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

enriched = (
    latest_match_products
    .withColumn("prev_cost", F.lag("cost", 2).over(w7))
    .withColumn("inventory_quantity_bucket", F.ntile(5).over(w8))
    .withColumn("prev_next_price", F.lag("next_price", 1).over(w9))
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id") == 5), "left_anti")
)

result = (
    not_flagged
    .filter(F.col("next_price").isNull())
    .select(
        "id",
        "category",
        "cost",
        "prev_cost",
        "inventory_quantity_bucket",
        "prev_next_price",
        F.lag("inventory_quantity_bucket", 1).over(w10).alias("prev_inventory_quantity_bucket"),
        F.lead("prev_next_price", 1, 0).over(w11).alias("next_prev_next_price"),
        F.avg("stock_qty").over(w12).alias("stock_qty_avg_2_prior"),
    )
)
