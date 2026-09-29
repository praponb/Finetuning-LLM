from pyspark.sql import functions as F

not_flagged = (
    df_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") >= F.col("s.launch_date")), "left_anti")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("cost") >= 790, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
        "category",
    )
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.sum(F.when(F.col("category") == "Grocery", F.col("stock_qty"))).alias("grocery_total_stock_qty"),
        F.max("cost").alias("max_cost"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
    )
    .filter(F.col("grocery_total_stock_qty") > 0)
    .orderBy(F.col("max_cost").asc_nulls_last())
)
