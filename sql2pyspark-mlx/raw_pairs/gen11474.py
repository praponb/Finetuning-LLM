from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("brand", "supplier_id").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("price", "id")

with_windows = (
    df_products
    .filter(F.col("stock_qty") != 299)
    .select(
        "id",
        "product_name",
        "category",
        "cost",
        "stock_qty",
        "launch_date",
        F.max("price").over(w_recent).alias("running_max_price"),
        F.sum("price").over(w_recent).alias("running_sum_price"),
        F.count("price").over(w_recent).alias("running_count_price"),
        F.lead("cost", 1, 0).over(w2).alias("next_cost"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "product_name",
        "category",
        "cost",
        "stock_qty",
        "launch_date",
        "running_max_price",
        "running_sum_price",
        "running_count_price",
    )
    .orderBy(F.col("running_sum_price").desc())
)
