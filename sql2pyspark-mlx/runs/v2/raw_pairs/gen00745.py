from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("supplier_id") == 36) & F.col("supplier_id").isNotNull())
    .groupBy("brand")
    .agg(
        F.sum(F.when(F.col("category") == "Grocery", F.col("stock_qty")).otherwise(0)).alias("grocery_stock_qty"),
        F.sum(F.when(F.col("category") == "Home", F.col("cost")).otherwise(0)).alias("home_cost"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
    )
    .orderBy("home_cost", "grocery_stock_qty", "brand")
    .limit(3)
)
