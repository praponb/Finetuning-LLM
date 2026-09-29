from pyspark.sql import functions as F

result = (
    df_products
    .agg(
        F.sum(F.when(F.col("brand") == "Acme", F.col("cost")).otherwise(0)).alias("acme_cost"),
    )
    .orderBy("acme_cost")
)
