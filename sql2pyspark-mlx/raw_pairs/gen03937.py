from pyspark.sql import functions as F

result = (
    df_products
    .alias("s")
    .join(df_suppliers.alias("dim"), F.col("s.supplier_id") == F.col("dim.id"), "left")
    .groupingSets(
        [["city", "brand"], ["city"]],
        F.col("dim.city").alias("city"),
        F.col("s.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("s.cost")).alias("max_cost"),
        F.avg(F.col("s.stock_qty")).alias("avg_stock_qty"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("avg_stock_qty").desc_nulls_first())
)
