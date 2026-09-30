from pyspark.sql import functions as F

filtered_inventory = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 3]) | F.col("store_id").isin([2, 4, 6]))
    .withColumn("last_restock_period", F.date_trunc("month", "last_restock"))
)

agg = (
    filtered_inventory
    .groupBy(
        F.when(F.year("last_restock_period") < 2024, "before_2024").when(F.year("last_restock_period") == 2024, "2024").otherwise("after_2024").alias("last_restock_period_cohort"),
    )
    .agg(F.countDistinct("quantity").alias("unique_products"))
)

result = (
    agg
    .select("last_restock_period_cohort", "unique_products")
    .orderBy("last_restock_period_cohort")
)
