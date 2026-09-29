from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())

latest_match = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"), F.col("c.unit_price"))
)

cleaned = (
    latest_match
    .filter(F.col("launch_date") > F.lit("2023-08-07").cast("date"))
    .withColumn("launch_date_dayofweek", F.dayofweek("launch_date"))
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.brand",
        "s.price",
        "s.stock_qty",
        "s.launch_date",
        "s.tags",
        "s.supplier_id",
        "s.unit_price",
        F.col("c.units").alias("store_sale_units"),
        F.col("c.sale_date").alias("store_sale_sale_date"),
    )
)

eligible = (
    with_latest
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.sale_date") <= F.col("s.launch_date")), "left_anti")
)

result = (
    eligible
    .withColumn("unit_price_tier", F.when(F.col("unit_price") >= 320, "HIGH").otherwise("LOW"))
    .withColumn("store_sale_units_tier", F.when(F.col("store_sale_units") >= 37, "PREMIUM").otherwise("STANDARD"))
    .withColumn("activity_month", F.date_trunc("month", "store_sale_sale_date"))
    .cube("unit_price_tier", "store_sale_units_tier", "activity_month")
    .agg(
        F.sum(F.when(F.col("brand") == "Stark", F.col("unit_price")).otherwise(0)).alias("stark_unit_price"),
        F.count("*").alias("row_count"),
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_count"),
        F.grouping_id().alias("grouping_id"),
    )
)
