from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("margin", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("supplier_id").orderBy(F.col("running_count_price").desc_nulls_last(), "id")
w3 = Window.partitionBy("brand", "supplier_id").orderBy("latest_rank", "id")

filtered = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-06-03").cast("date"))
    .select(
        "*",
        (F.col("cost") * F.col("price")).alias("margin"),
        F.coalesce("brand", F.lit("N/A")).alias("brand_filled"),
    )
)

with_windows = (
    filtered
    .withColumn("running_count_price", F.count("price").over(w))
    .withColumn("running_min_price", F.min("price").over(w))
    .withColumn("running_sum_price", F.sum("price").over(w))
)

deduped = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

latest = (
    deduped
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "product_name",
        "brand",
        "stock_qty",
        "tags",
        "supplier_id",
        "margin",
        "brand_filled",
        "running_count_price",
        "running_sum_price",
        "latest_rank",
    )
)

result = (
    latest
    .select(
        "id",
        "brand",
        "stock_qty",
        "brand_filled",
        "running_count_price",
        "latest_rank",
        F.when(F.col("brand").rlike("^(Globex|Initech)$"), "OTHER_ZONE").when(F.col("brand").rlike("^(Acme|Umbrella)$"), "GROUP_B").otherwise("CORE").alias("brand_zone"),
    )
    .orderBy(F.col("brand_zone").desc(), F.col("stock_qty").desc())
)
