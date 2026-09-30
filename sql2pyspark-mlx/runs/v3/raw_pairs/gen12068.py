from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("Des%") & (F.col("price") < 830) & (F.col("category") == "Fashion"))
    .groupBy("category")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("brand") == "Umbrella", 1).otherwise(0))) / F.count("*")), 1).alias("pct_umbrella"),
        F.countDistinct("stock_qty").alias("unique_brands"),
        F.count("cost").alias("cost_count"),
    )
    .orderBy(F.col("unique_brands").desc(), "pct_umbrella")
)
