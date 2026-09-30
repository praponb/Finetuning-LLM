from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id")
w2 = Window.partitionBy("brand")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())

base = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-04-16").cast("date")) & (F.col("supplier_id") == 25))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.array_join("tags", "|").alias("tags_text"),
        (F.col("cost") * F.col("price")).alias("margin"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.count("stock_qty").over(w).alias("supplier_id_count_stock_qty"),
        F.sum("margin").over(w2).alias("brand_sum_margin"),
    )
)

with_dims = (
    metrics
    .alias("s")
    .join(df_suppliers.alias("b"), F.col("s.supplier_id") == F.col("b.id"), "inner")
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.brand",
        "s.stock_qty",
        "s.supplier_id",
        "s.supplier_id_count_stock_qty",
        "s.brand_sum_margin",
        F.col("b.rating"),
        F.col("b.supplier_name"),
        F.col("b.city"),
    )
)

cleaned = (
    with_dims
    .filter(F.col("supplier_id_count_stock_qty") > 7)
    .select(
        "id",
        "category",
        "brand",
        "supplier_id",
        "supplier_id_count_stock_qty",
        "rating",
        "supplier_name",
        "city",
        (F.col("stock_qty") + F.col("supplier_id_count_stock_qty")).alias("stock_qty_plus_supplier_id_count_stock_qty"),
        F.coalesce("brand", F.lit("Unknown")).alias("brand_filled"),
        F.coalesce("brand", F.lit("NONE")).alias("resolved_brand"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.brand",
        "s.rating",
        "s.supplier_name",
        "s.brand_filled",
        "s.resolved_brand",
        F.col("c.sale_date"),
    )
)

result = (
    with_last
    .groupBy("category")
    .agg(
        F.avg(F.when(F.col("brand") == "Stark", F.col("rating"))).alias("stark_avg_rating"),
        F.sum("rating").alias("total_rating"),
    )
)
