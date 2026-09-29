from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .select(
        F.col("s.country"),
        F.col("s.rating"),
        F.col("s.city"),
        F.col("p.stock_qty"),
        F.col("p.price"),
        F.col("p.launch_date"),
    )
    .orderBy("rating", "launch_date")
)
