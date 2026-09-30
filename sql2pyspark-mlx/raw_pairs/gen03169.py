from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .filter(F.month(F.col("products.launch_date")).isin([4, 5, 9]))
    .select(
        F.col("order_items.id"),
        F.col("order_items.order_id"),
        F.col("products.product_name"),
        F.col("products.cost"),
        F.col("suppliers.supplier_name"),
        F.col("suppliers.country"),
    )
    .orderBy("cost", F.col("order_id").desc())
)
