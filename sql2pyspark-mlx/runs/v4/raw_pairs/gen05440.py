from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("margin", "id")

cleaned = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2023-04-16").cast("date"))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        (F.col("price") * F.col("cost")).alias("margin"),
        F.trim("product_name").alias("clean_product_name"),
        F.coalesce("brand", F.lit("N/A")).alias("brand_clean"),
    )
)

result = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
