from pyspark.sql import functions as F

filtered_products = df_products.filter(F.col("stock_qty") < 174)

products_stats = (
    filtered_products
    .groupBy("category")
    .agg(
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_count"),
        F.sum(F.when(F.col("category") == "Grocery", F.col("cost")).otherwise(0)).alias("grocery_cost"),
    )
)

result = (
    products_stats
    .filter(F.col("grocery_cost") > 43)
    .select("category", "umbrella_count", "grocery_cost")
    .orderBy(F.col("grocery_cost").desc())
)
