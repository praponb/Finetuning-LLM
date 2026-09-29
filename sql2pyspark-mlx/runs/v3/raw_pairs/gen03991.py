from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.alias("products"), F.col("suppliers.id") == F.col("products.supplier_id"), "left")
    .groupBy(F.col("suppliers.id"))
    .agg(
        F.count(F.col("products.id")).alias("product_count"),
        F.sum(F.col("products.cost")).alias("total_cost"),
        F.sum(F.col("products.stock_qty")).alias("total_stock_qty"),
    )
    .orderBy("id", "total_cost")
    .limit(10)
)
