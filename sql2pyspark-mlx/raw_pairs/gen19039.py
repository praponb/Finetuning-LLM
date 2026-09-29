from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
        F.date_trunc("MONTH", "last_restock").alias("last_restock_month"),
    )
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.count("quantity").alias("quantity_count"),
        F.max("last_restock").alias("latest_last_restock"),
    )
    .orderBy(
        F.col("quantity_count").desc(),
        "last_restock_cohort",
        "last_restock_month",
    )
    .limit(20)
)
