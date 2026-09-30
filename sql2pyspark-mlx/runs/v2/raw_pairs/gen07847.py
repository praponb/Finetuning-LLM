from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "right")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "outer")
    .filter(F.col("products.stock_qty").between(150, 167))
    .select(
        F.col("order_items.product_id"),
        F.col("order_items.quantity"),
        F.col("products.cost"),
        F.col("products.brand"),
        F.col("suppliers.rating"),
        F.concat_ws(",", F.col("suppliers.city"), F.col("suppliers.country")).alias("city_country_key"),
    )
)
