from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("launch_date")).orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("category")
w3 = Window.partitionBy("supplier_id")
w4 = Window.partitionBy("category").orderBy("cost", "id")
w5 = Window.orderBy(F.col("price_filled").desc_nulls_last())

cleaned = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .withColumn("price_filled", F.coalesce("price", F.lit(0)))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("OTHER")))
)

result = (
    cleaned
    .select(
        "id",
        "product_name",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        "price_filled",
        F.sum("price").over(w).alias("cumulative_price"),
        F.count("price").over(w_part).alias("category_count_price"),
        F.min("cost").over(w3).alias("supplier_id_min_cost"),
        F.ntile(10).over(w4).alias("cost_bucket"),
        F.rank().over(w5).alias("price_filled_rank"),
    )
)
