from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("supplier_id", "category").orderBy(F.col("price").asc_nulls_first())
w2 = Window.partitionBy("category").orderBy("price", "id")
w3 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.month("launch_date"), "brand")
w6 = Window.partitionBy("brand", "supplier_id").orderBy("running_stock_qty", "id").rowsBetween(-3, -1)
w7 = Window.partitionBy("supplier_id", "category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("supplier_id").orderBy("first_known_price", "id")
w9 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w10 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())

scored = (
    df_products
    .select(
        "*",
        F.percent_rank().over(w_recent).alias("price_percent_rank"),
        F.row_number().over(w2).alias("event_seq"),
        F.first("price", ignorenulls=True).over(w3).alias("first_known_price"),
        F.sum("stock_qty").over(w4).alias("running_stock_qty"),
        F.max("price").over(w5).alias("launch_date_month_max_price"),
    )
)

with_windows = (
    scored
    .withColumn("event_seq_avg_3_prior", F.avg("event_seq").over(w6))
    .withColumn("running_event_seq", F.sum("event_seq").over(w7))
    .withColumn("first_known_price_bucket", F.ntile(3).over(w8))
)

enriched = (
    with_windows
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id").eqNullSafe(F.col("b.id")), "left")
    .filter(F.length(F.col("b.supplier_name")) < 10)
    .select(
        "e.id",
        "e.category",
        "e.price",
        "e.cost",
        "e.stock_qty",
        "e.launch_date",
        "e.supplier_id",
        "e.price_percent_rank",
        "e.event_seq",
        "e.event_seq_avg_3_prior",
        "e.running_event_seq",
        "e.first_known_price_bucket",
        F.col("b.country"),
        F.col("b.rating"),
        F.col("b.city"),
    )
)

with_dims = (
    enriched
    .alias("a")
    .join(df_suppliers.alias("d"), F.col("a.supplier_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "a.id",
        "a.category",
        "a.stock_qty",
        "a.launch_date",
        "a.supplier_id",
        "a.running_event_seq",
        "a.rating",
        F.col("d.country").alias("supplier_country"),
        F.col("d.rating").alias("supplier_rating"),
        F.col("d.supplier_name"),
    )
)

latest_match = (
    with_dims
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.stock_qty",
        "s.launch_date",
        "s.rating",
        "s.supplier_country",
        "s.supplier_name",
        F.col("c.unit_price").alias("order_item_unit_price"),
    )
)

latest_match_products = (
    latest_match
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w10))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.sale_date").alias("store_sale_sale_date"),
        F.col("c.units").alias("store_sale_units"),
    )
)

result = (
    latest_match_products
    .select(
        "id",
        "stock_qty",
        "launch_date",
        "rating",
        "supplier_country",
        "supplier_name",
        "order_item_unit_price",
        "store_sale_sale_date",
        "store_sale_units",
    )
)
