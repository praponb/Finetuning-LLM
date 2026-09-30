from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("price").desc_nulls_last(), "id")
w2 = Window.partitionBy("city").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.year("launch_date"), "supplier_city").orderBy("row_num", "id").rowsBetween(-5, Window.currentRow)
w_grp = Window.partitionBy("brand").orderBy(F.col("rating").asc_nulls_last())
w5 = Window.orderBy("row_num", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("city")

valid = (
    df_products
    .filter(F.col("stock_qty").between(288, 362))
    .withColumn("brand_product_name_key", F.concat_ws("/", "brand", "product_name"))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("N/A")))
)

enriched = (
    valid
    .alias("src")
    .join(df_suppliers.alias("d"), F.col("src.supplier_id").eqNullSafe(F.col("d.id")), "left")
    .filter(F.lower(F.trim(F.col("d.supplier_name"))) == "pacific wholesale")
    .select(
        "src.id",
        "src.product_name",
        "src.brand",
        "src.price",
        "src.launch_date",
        "src.tags",
        "src.supplier_id",
        "src.brand_filled",
        F.col("d.city"),
        F.col("d.supplier_name"),
        F.col("d.rating"),
    )
)

valid_products = (
    enriched
    .filter((F.col("launch_date") >= F.lit("2023-09-13").cast("date")) & F.col("city").isNotNull())
    .withColumn("clean_brand", F.trim("brand"))
    .withColumn("brand_clean", F.coalesce("brand", F.lit("OTHER")))
)

latest = (
    valid_products
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_dims = (
    latest
    .alias("src")
    .join(df_suppliers.alias("s"), F.col("src.supplier_id") == F.col("s.id"), "left")
    .select("src.*", F.col("s.city").alias("supplier_city"))
)

with_windows = (
    with_dims
    .select(
        "id",
        "product_name",
        "price",
        "launch_date",
        "tags",
        "brand_filled",
        "city",
        "supplier_name",
        "clean_brand",
        "row_num",
        F.sum("price").over(w2).alias("running_price"),
        F.avg("price").over(w3).alias("price_avg_5"),
        F.cume_dist().over(w_grp).alias("rating_cume_dist"),
        F.avg("rating").over(w5).alias("rating_avg_6"),
        F.round((F.col("price") / F.sum("price").over(w6)), 4).alias("price_share"),
    )
)

result = (
    with_windows
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_rows"),
        F.count("*").alias("event_count"),
    )
)
