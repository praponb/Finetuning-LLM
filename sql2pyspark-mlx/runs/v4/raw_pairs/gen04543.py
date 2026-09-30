from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("supplier_id")

filtered = (
    df_products
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-11-27"))) & F.col("supplier_id").isNotNull())
    .select(
        "*",
        F.substring("product_name", 1, 2).alias("product_name_prefix"),
        F.size("tags").alias("tags_count"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_filled"),
    )
)

cleaned = (
    filtered
    .filter(F.col("launch_date") >= F.to_date(F.lit("2023-12-19")))
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
        "brand_filled",
        F.weekofyear("launch_date").alias("launch_date_weekofyear"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.min("price").over(w2).alias("supplier_id_min_price"),
    )
)

result = (
    scored
    .groupBy("supplier_id")
    .pivot("category", ["Electronics", "Home", "Books"])
    .agg(F.max("launch_date_weekofyear"))
)
