from pyspark.sql import functions as F

src = df_inventory.filter(F.col("store_id").isin([1, 2, 5, 6]))

summary = (
    src
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
    )
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.min("last_restock").alias("first_last_restock"),
    )
)

result = summary.select("last_restock_cohort", "first_last_restock")
