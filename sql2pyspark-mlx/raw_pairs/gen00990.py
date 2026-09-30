from pyspark.sql import functions as F

by_dimension = (
    df_order_items
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .groupingSets(
        [["brand", "category"], ["brand"], []],
        F.col("p.brand").alias("brand"),
        F.col("p.category").alias("category"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("src.quantity")).alias("max_quantity"),
        F.min(F.col("src.unit_price")).alias("min_unit_price"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    by_dimension
    .select(
        "brand",
        "category",
        "max_quantity",
        "grouping_level",
        F.when(F.col("brand").rlike("^(Umbrella|Globex)$"), "EXTENDED").otherwise("PRIORITY").alias("brand_zone"),
    )
    .orderBy(F.col("max_quantity").desc(), "brand")
)
