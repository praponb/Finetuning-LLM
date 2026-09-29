from pyspark.sql import functions as F

result = (
    df_products
    .filter(~(F.col("cost").between(240, 580) & (F.length("product_name") >= 10)))
    .groupBy("category", "brand")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("category") == "Grocery", 1).otherwise(0))) / F.count("*")), 1).alias("pct_grocery"),
    )
)
