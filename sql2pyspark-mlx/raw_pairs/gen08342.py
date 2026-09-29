from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("region", "store_id").orderBy(F.col("quantity").asc_nulls_last())
w_ordered = Window.partitionBy("product_id", "store_id").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.to_date("last_restock_period"), "region").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id")
w6 = Window.partitionBy("product_id").orderBy("quantity", "id")
w7 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")

base = (
    df_inventory
    .filter(F.col("store_id") == 4)
    .withColumn("last_restock_period", F.date_trunc("week", "last_restock"))
)

joined = (
    base
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.region"), F.col("b.sqft"))
)

metrics = (
    joined
    .filter(F.col("last_restock_period") < "2024-07-22")
    .select(
        "*",
        F.dense_rank().over(w_grp).alias("quantity_dense_rank"),
        F.last("region", ignorenulls=True).over(w_ordered).alias("last_known_region"),
        F.sum("quantity").over(w3).alias("cumulative_quantity"),
    )
)

enriched = (
    metrics
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.product_id",
        "s.quantity",
        "s.sqft",
        "s.last_known_region",
        "s.cumulative_quantity",
        F.col("p.product_name"),
        F.col("p.stock_qty"),
    )
)

result = (
    enriched
    .select(
        "id",
        "product_id",
        "quantity",
        "last_known_region",
        "product_name",
        "stock_qty",
        F.first("stock_qty", ignorenulls=True).over(w4).alias("first_known_stock_qty"),
        F.stddev_samp("stock_qty").over(w5).alias("product_id_stddev_samp_stock_qty"),
        F.lag("sqft", 1, 0).over(w6).alias("prev_sqft"),
        F.ntile(4).over(w7).alias("quantity_bucket"),
    )
)
