from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNull() | F.col("cost").between(370, 440))
    .select(
        "id",
        "category",
        "price",
        (F.col("cost") + F.col("stock_qty")).alias("cost_plus_stock_qty"),
        F.round((F.col("cost") / F.col("price")), 2).alias("margin"),
    )
    .orderBy(F.col("id").desc())
)
