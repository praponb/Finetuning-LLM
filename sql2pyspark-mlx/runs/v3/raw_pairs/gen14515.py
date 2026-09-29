from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "right")
    .filter(F.col("s.sqft").between(5776, 6809) & F.col("p.cost").between(450, 630))
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("ss.units"),
        F.col("p.stock_qty"),
        F.col("s.store_name"),
        F.col("s.city"),
    )
    .orderBy("store_name")
)
