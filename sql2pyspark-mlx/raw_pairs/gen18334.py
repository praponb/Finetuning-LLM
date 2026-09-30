from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("category").isin(["Sports", "Home"]))
    .groupBy("supplier_id")
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.round(F.stddev("price"), 2).alias("stddev_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.stddev_price") > 35)
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.col("ss.min_launch_date"),
        F.col("ss.stddev_price"),
    )
    .orderBy(F.col("stddev_price").desc(), "id")
    .limit(10)
)
