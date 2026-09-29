from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc_nulls_last())
w3 = Window.orderBy(F.col("price").asc_nulls_last())

valid = (
    df_products
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-08-10"))) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        F.concat("category", F.lit(" - "), "brand").alias("category_brand"),
    )
)

result = (
    valid
    .select(
        "*",
        F.sum("price").over(w).alias("running_price"),
        F.percent_rank().over(w2).alias("stock_qty_percent_rank"),
        F.percent_rank().over(w3).alias("price_percent_rank"),
    )
)
