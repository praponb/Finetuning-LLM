from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("category").isin(["Sports", "Fashion"]), "priority").otherwise("other").alias("category_group"),
    )
    .agg(
        F.sum(F.when(F.col("category") == "Grocery", 1).otherwise(0)).alias("grocery_count"),
        F.sum("cost").alias("total_cost"),
    )
    .filter(F.col("total_cost") > 2400)
    .drop("total_cost")
)
