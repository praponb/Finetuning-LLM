from pyspark.sql import functions as F

by_dimension = (
    df_products
    .alias("src")
    .join(df_suppliers.alias("d"), F.col("src.supplier_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["city", "country", "category"], ["city"]],
        F.col("d.city").alias("city"),
        F.col("d.country").alias("country"),
        F.col("src.category").alias("category"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("src.price")), 2).alias("total_price"),
        F.round(F.avg(F.col("src.cost")), 2).alias("avg_cost"),
        F.grouping_id().alias("level_id"),
    )
)

result = (
    by_dimension
    .select("country", "num_rows", "total_price", "level_id")
    .orderBy(F.col("country").desc())
)
