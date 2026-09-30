from pyspark.sql import functions as F

dim_totals = (
    df_products
    .alias("src")
    .join(df_suppliers.alias("s"), F.col("src.supplier_id") == F.col("s.id"), "left")
    .groupingSets(
        [["country", "brand"], ["country"], []],
        F.col("s.country").alias("country"),
        F.col("src.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("src.price")), 2).alias("total_price"),
        F.grouping_id().alias("level_id"),
    )
)

result = (
    dim_totals
    .select("country", "brand", "cnt", "total_price", "level_id")
    .orderBy("cnt")
)
