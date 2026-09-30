from pyspark.sql import functions as F

src = (
    df_products
    .filter(F.size("tags") >= 1)
    .withColumn("product_name_slug", F.regexp_replace("product_name", " ", "_"))
)

agg = (
    src
    .groupBy("category")
    .agg(
        F.max("launch_date").alias("last_launch_date"),
        F.sum(F.when(F.col("category") == "Grocery", F.col("cost")).otherwise(0)).alias("grocery_cost"),
    )
)

result = (
    agg
    .filter(F.col("grocery_cost") >= 80)
    .select("category", "last_launch_date", "grocery_cost")
    .orderBy("last_launch_date", "category")
    .limit(5)
)
