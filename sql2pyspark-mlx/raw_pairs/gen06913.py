from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")
w2 = Window.partitionBy("brand", "category").orderBy(F.col("stock_qty").desc())
w3 = Window.partitionBy("category").orderBy("price", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())

enriched = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "*",
        F.corr("price", "cost").over(w).alias("corr_price_cost"),
        F.rank().over(w2).alias("stock_qty_rank"),
        F.ntile(3).over(w3).alias("price_bucket"),
    )
)

with_latest = (
    enriched
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.price",
        "s.cost",
        "s.launch_date",
        "s.supplier_id",
        "s.stock_qty_rank",
        "s.price_bucket",
        F.col("c.unit_price"),
    )
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.launch_date",
        "s.supplier_id",
        "s.stock_qty_rank",
        "s.price_bucket",
        "s.unit_price",
        F.col("c.units"),
        F.col("c.sale_date"),
    )
)

joined = (
    latest_match
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.product_name",
        "a.category",
        "a.launch_date",
        "a.supplier_id",
        "a.stock_qty_rank",
        "a.price_bucket",
        "a.sale_date",
        F.col("p.city"),
    )
)

result = (
    joined
    .groupBy(
        F.when(F.col("stock_qty_rank") >= 8, "LARGE").otherwise("SMALL").alias("stock_qty_rank_tier"),
    )
    .agg(
        F.count_if(F.col("category") == "Home").alias("home_events"),
        F.round(F.avg("price_bucket"), 2).alias("avg_price_bucket"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
    .filter(F.col("num_cities") >= 1)
    .orderBy(F.col("num_cities").asc_nulls_first())
)
