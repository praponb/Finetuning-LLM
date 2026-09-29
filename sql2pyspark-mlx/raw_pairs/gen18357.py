from pyspark.sql import functions as F

stock_qty_by_supplier_id = df_products.groupBy("supplier_id").agg(F.sum("stock_qty").alias("sum_product_stock_qty"))

eligible = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") >= "2024-10-01"), "left_anti")
)

result = (
    eligible
    .join(stock_qty_by_supplier_id, on="supplier_id", how="left")
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
        F.col("sum_product_stock_qty"),
    )
    .orderBy(F.col("sum_product_stock_qty").desc(), F.col("id").desc())
)
