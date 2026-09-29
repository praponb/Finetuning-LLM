from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.month(F.col("p.launch_date")).isin([1, 5, 9]))
    .groupBy(
        F.when(F.year(F.col("p.launch_date")) < 2024, "before_2024").when(F.year(F.col("p.launch_date")) == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
        F.when(F.col("p.brand").isin(["Acme", "Umbrella"]), "core").otherwise("other").alias("brand_group"),
    )
    .agg(
        F.min(F.col("ss.revenue")).alias("min_revenue"),
        F.min(F.col("ss.units")).alias("min_units"),
    )
    .orderBy("brand_group")
)
