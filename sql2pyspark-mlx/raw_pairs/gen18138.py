from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .select(
        F.col("s.rating"),
        F.col("p.launch_date"),
        (F.col("p.cost") + F.col("p.stock_qty")).alias("cost_plus_stock_qty"),
    )
)
