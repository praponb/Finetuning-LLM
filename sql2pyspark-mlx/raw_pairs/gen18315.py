from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.sum(F.when(F.col("category") == "Home", F.col("cost")).otherwise(0)).alias("home_cost"),
    )
    .filter(F.col("home_cost") < 21)
)
