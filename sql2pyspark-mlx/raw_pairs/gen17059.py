from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("brand").orderBy("units")

filtered = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-06-06").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "tags",
        "supplier_id",
        F.regexp_replace("product_name", " ", "").alias("product_name_slug"),
        F.coalesce("brand", F.lit("OTHER")).alias("resolved_brand"),
    )
)

with_latest = (
    filtered
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.brand",
        "s.price",
        "s.stock_qty",
        "s.tags",
        "s.product_name_slug",
        "s.resolved_brand",
        F.col("c.quantity"),
        F.col("c.unit_price"),
    )
)

base = (
    with_latest
    .filter(F.col("brand") == "Stark")
    .select(
        "id",
        "brand",
        "price",
        "stock_qty",
        "quantity",
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
        F.coalesce("brand", F.lit("N/A")).alias("brand_filled"),
    )
)

latest_match = (
    base
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.brand",
        "s.stock_qty",
        "s.brand_filled",
        F.col("c.units"),
        F.col("c.sale_date").alias("store_sale_sale_date"),
    )
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.revenue") < 2000), "left_semi")
)

ranked = (
    not_flagged
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select("id", "brand", "stock_qty", "units", "store_sale_sale_date")
)

result = (
    ranked
    .groupBy(F.when(F.col("units") >= 38, "HIGH").otherwise("LOW").alias("units_tier"))
    .agg(
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.stddev_samp("stock_qty").alias("stock_qty_stddev"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("stock_qty_stddev") >= 0)
    .orderBy("avg_units")
)
