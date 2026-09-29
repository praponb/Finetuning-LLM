from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("price").desc_nulls_first())
w2 = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc_nulls_last())
w3 = Window.partitionBy("brand", "category").orderBy("stock_qty", "id")

base = (
    df_products
    .filter(~F.col("brand").eqNullSafe(F.lit("Umbrella")))
    .withColumn("clean_product_name", F.trim("product_name"))
    .withColumn("brand_clean", F.coalesce("brand", F.lit("OTHER")))
)

result = (
    base
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
        "brand_clean",
        F.cume_dist().over(w).alias("price_cume_dist"),
        F.percent_rank().over(w2).alias("stock_qty_percent_rank"),
        F.row_number().over(w3).alias("seq_no"),
    )
)
