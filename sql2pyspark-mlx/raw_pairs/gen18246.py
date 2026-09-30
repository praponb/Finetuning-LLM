from pyspark.sql import functions as F

result = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "inner")
    .groupingSets(
        [["city", "brand"], ["city"]],
        F.col("s.city").alias("city"),
        F.col("a.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("a.cost")).alias("min_cost"),
        F.round(F.avg(F.col("a.price")), 2).alias("avg_price"),
        F.grouping_id().alias("level_id"),
    )
)
