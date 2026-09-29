from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("price", "id")
w2 = Window.partitionBy("brand").orderBy(F.col("price").desc(), "id")
w3 = Window.partitionBy("category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("supplier_id").orderBy(F.col("price").desc(), "id")

filtered = (
    df_products
    .filter((F.col("price") < 1020) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        F.when(F.col("stock_qty") >= 393, "High").when(F.col("stock_qty") >= 62, "Medium").otherwise("Low").alias("stock_qty_band"),
    )
)

deduped = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_windows = (
    deduped
    .select(
        "*",
        F.ntile(5).over(w2).alias("price_bucket"),
        F.sum("price").over(w3).alias("running_price"),
    )
)

valid = (
    with_windows
    .filter((F.col("launch_date") > F.lit("2023-05-27").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "*",
        F.array_contains("tags", "clearance").alias("is_clearance"),
        F.floor("price").alias("price_whole"),
    )
)

latest = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

valid_products = (
    latest
    .filter(F.col("launch_date") > F.lit("2023-04-20").cast("date"))
    .select(
        "*",
        F.coalesce("running_price", F.lit(0)).alias("running_price_filled"),
        F.date_trunc("WEEK", "launch_date").alias("launch_date_period"),
    )
)

result = (
    valid_products
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "launch_date",
        "tags",
        "stock_qty_band",
        "price_bucket",
        F.when(F.col("product_name").rlike("^(Office Chair|Noise-Cancel Headphones)$"), "PRIORITY").when(F.col("product_name").rlike("^(Laptop Pro 14|wireless mouse)$"), "CORE").otherwise("EXTENDED").alias("product_name_zone"),
    )
    .orderBy("brand")
)
