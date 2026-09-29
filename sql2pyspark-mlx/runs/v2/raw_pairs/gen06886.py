from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") == "Umbrella")
    .groupBy("category", "supplier_id")
    .agg(
        F.sum(F.when(F.col("category") == "Sports", F.col("cost")).otherwise(0)).alias("sports_cost"),
        (F.max("price") - F.min("price")).alias("price_range"),
    )
    .filter(F.col("price_range") < 27)
)
