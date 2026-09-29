from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 10]) & F.col("product_id").isin([1, 5, 7]))
    .groupBy(
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"),
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
    )
    .agg(
        F.max("last_restock").alias("latest_last_restock"),
        F.max("last_restock").alias("last_last_restock"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .orderBy("last_last_restock")
)
