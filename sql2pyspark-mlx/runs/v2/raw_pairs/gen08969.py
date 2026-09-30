from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("cost") <= 630) & F.array_contains("tags", "sale"))
    .groupBy("supplier_id", F.date_trunc("MONTH", "launch_date").alias("launch_month"))
    .agg(
        F.min("stock_qty").alias("min_stock_qty"),
        F.countDistinct("cost").alias("unique_suppliers"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
    )
    .filter(F.col("min_stock_qty") < 240)
)
