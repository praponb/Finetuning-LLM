from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .groupBy(
        F.when(F.year(F.col("p.launch_date")) < 2024, "before_2024").when(F.year(F.col("p.launch_date")) == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
        F.date_format(F.col("p.launch_date"), "yyyy-MM").alias("launch_month"),
    )
    .agg(
        F.countDistinct(F.col("i.quantity")).alias("unique_products"),
        F.max(F.col("i.quantity")).alias("max_quantity"),
    )
    .filter(F.col("unique_products") > 1)
    .orderBy("launch_cohort")
)
