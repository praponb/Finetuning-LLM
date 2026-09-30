from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("stock_qty", "id")
w2 = Window.partitionBy("supplier_id").orderBy(F.col("prev_price").desc(), "id")
w3 = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc(), "id")

valid = (
    df_products
    .filter(F.col("launch_date") >= F.to_date(F.lit("2023-04-19")))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        F.array_contains("tags", "premium").alias("is_premium"),
        F.coalesce("brand", F.lit("N/A")).alias("brand_clean"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "product_name",
        "brand",
        "stock_qty",
        "tags",
        "supplier_id",
        F.lag("price").over(w).alias("prev_price"),
        F.lead("stock_qty").over(w).alias("next_stock_qty"),
        F.row_number().over(w).alias("event_seq"),
    )
)

deduped = (
    with_windows
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("id", "product_name", "brand", "stock_qty", "prev_price", "next_stock_qty")
)

deduped_products = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped_products
    .select("id", "brand", "prev_price")
    .orderBy(F.col("id").desc_nulls_first())
)
