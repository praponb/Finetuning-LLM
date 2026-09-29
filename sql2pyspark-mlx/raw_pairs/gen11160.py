from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("price").desc(), "id")
w_ordered = Window.partitionBy("category")
price_by_supplier_id = df_products.groupBy("supplier_id").agg(F.sum("price").alias("sum_product_price"))

with_windows = (
    df_products
    .filter(F.col("product_name").isNotNull())
    .select(
        "id",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.row_number().over(w).alias("row_seq"),
        F.corr("stock_qty", "price").over(w_ordered).alias("corr_stock_qty_price"),
    )
)

result = (
    with_windows
    .join(price_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "price",
        "stock_qty",
        "supplier_id",
        "corr_stock_qty_price",
        F.col("sum_product_price"),
    )
)
