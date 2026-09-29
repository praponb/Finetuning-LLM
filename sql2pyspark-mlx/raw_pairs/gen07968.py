from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("product_name_length").desc_nulls_first())

cleaned = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2023-07-05").cast("date"))
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.length("product_name").alias("product_name_length"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_filled"),
    )
)

result = (
    cleaned
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)
