from pyspark.sql import functions as F

rollup_totals = (
    df_products
    .alias("src")
    .join(df_suppliers.alias("s"), F.col("src.supplier_id") == F.col("s.id"), "inner")
    .cube(F.col("s.city").alias("city"), F.col("src.brand").alias("brand"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("src.price")), 2).alias("total_price"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    rollup_totals
    .select(
        "city",
        "brand",
        "total_price",
        "grouping_level",
        F.when(F.col("city").rlike("^(New York|Berlin)$"), "PRIORITY").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("city_zone"),
    )
)
