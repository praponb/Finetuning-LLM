from pyspark.sql import functions as F

stock_qty_by_supplier_id = df_products.groupBy("supplier_id").agg(F.sum("stock_qty").alias("sum_product_stock_qty"))

kept = (
    df_products
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.quantity") <= 73), "left_anti")
)

result = (
    kept
    .join(stock_qty_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "category",
        "cost",
        "tags",
        "supplier_id",
        F.when(F.col("product_name").rlike("^(Laptop Pro 14|Running Shoes)$"), "CORE").when(F.col("product_name").rlike("^(wireless mouse|USB-C Hub)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("product_name_zone"),
        F.col("sum_product_stock_qty"),
    )
)
