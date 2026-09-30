from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .filter(F.col("revenue") >= 1300)
    .groupBy("product_id")
    .agg(
        F.sum("units").alias("total_units"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("p.category"),
        F.col("t.total_units"),
        F.col("t.units_range"),
    )
    .orderBy("category")
)
