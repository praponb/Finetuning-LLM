from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("launch_date", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("category")
w3 = Window.partitionBy("category", "brand").orderBy("cost", "id")

valid = (
    df_products
    .filter(F.col("launch_date") >= F.to_date(F.lit("2023-09-13")))
    .withColumn("product_name_normalized", F.upper(F.trim("product_name")))
)

result = (
    valid
    .select(
        "id",
        "product_name",
        "category",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        "product_name_normalized",
        F.avg("price").over(w).alias("price_avg_5_prior"),
        F.min("price").over(w2).alias("category_min_price"),
        F.lag("cost", 1).over(w3).alias("prev_cost"),
    )
)
