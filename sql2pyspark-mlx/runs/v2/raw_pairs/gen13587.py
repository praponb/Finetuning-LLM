from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("p.price").isNotNull() & F.col("p.cost").isNull())
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("p.stock_qty"),
        F.col("s.city"),
        F.when(F.col("p.cost") >= 780, "High").when(F.col("p.cost") >= 310, "Medium").otherwise("Low").alias("cost_level"),
    )
    .orderBy(F.col("stock_qty").desc())
)
