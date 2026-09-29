from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("p.launch_date") > "2023-09-30")
    .groupBy(F.col("s.id"), F.col("s.supplier_name"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.avg(F.col("p.stock_qty")).alias("avg_stock_qty"),
        F.max(F.col("p.cost")).alias("max_cost"),
    )
)
