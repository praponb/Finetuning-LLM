from pyspark.sql import functions as F

result = (
    df_products
    .agg(
        F.round(((100 * F.sum(F.when(F.col("brand") == "Acme", 1).otherwise(0))) / F.count("*")), 1).alias("pct_acme"),
    )
    .orderBy(F.col("pct_acme").desc())
)
