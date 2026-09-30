from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "inner")
    .groupBy(
        F.when(F.year(F.col("stores.opened_date")) < 2024, "before_2024").when(F.year(F.col("stores.opened_date")) == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(F.sum(F.col("inventory.quantity")).alias("total_quantity"))
    .orderBy(F.col("total_quantity").desc())
)
