from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("category").isin(["Fashion", "Grocery"]) & F.col("brand").isin(["Globex", "Umbrella", "Stark"]))
    .groupBy(
        "category",
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(
        F.max("stock_qty").alias("max_stock_qty"),
        F.count("stock_qty").alias("stock_qty_count"),
    )
)
