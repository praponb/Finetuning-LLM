from pyspark.sql import functions as F

stock_qty_by_supplier_id = df_products.groupBy("supplier_id").agg(F.max("stock_qty").alias("max_product_stock_qty"))

not_flagged = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.sale_date") <= F.col("s.launch_date")), "left_anti")
)

result = (
    not_flagged
    .join(stock_qty_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "price",
        "launch_date",
        "tags",
        "supplier_id",
        F.col("max_product_stock_qty"),
    )
    .orderBy(F.col("supplier_id").asc_nulls_first())
)
