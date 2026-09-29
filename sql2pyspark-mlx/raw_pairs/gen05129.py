from pyspark.sql import functions as F

by_dimension = (
    df_inventory
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .groupingSets(
        [["category", "brand"], ["category"]],
        F.col("p.category").alias("category"),
        F.col("p.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("s.quantity")).alias("total_quantity"),
    )
)

result = (
    by_dimension
    .select(
        "category",
        "brand",
        F.when(F.col("brand").rlike("^(Umbrella|Globex)$"), "OTHER_ZONE").when(F.col("brand").rlike("^(Acme|Initech)$"), "CORE").otherwise("PRIORITY").alias("brand_zone"),
    )
)
