from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").isNotNull() & F.col("cost").between(490, 520) & F.col("supplier_id").isin([21, 35]))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("brand") == "Stark", 1).otherwise(0))) / F.count("*")), 1).alias("pct_stark"),
    )
)
