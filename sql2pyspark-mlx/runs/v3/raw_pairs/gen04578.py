from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("brand") == "Globex") & F.col("category").isin(["Home", "Fashion", "Electronics"]))
    .groupBy(
        F.date_format("launch_date", "yyyy-MM").alias("launch_month"),
        "brand",
        "category",
    )
    .agg(
        F.sum(F.when(F.col("brand") == "Globex", 1).otherwise(0)).alias("globex_count"),
        F.countDistinct("cost").alias("unique_categorys"),
        F.count("price").alias("price_count"),
    )
    .orderBy("price_count", "category")
)
