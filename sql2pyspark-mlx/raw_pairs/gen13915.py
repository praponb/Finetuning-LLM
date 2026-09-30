from pyspark.sql import functions as F

eligible = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.sale_date") < F.col("s.launch_date")), "left_semi")
)

result = (
    eligible
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
    )
    .orderBy(F.col("stock_qty").desc_nulls_last(), "launch_date")
)
