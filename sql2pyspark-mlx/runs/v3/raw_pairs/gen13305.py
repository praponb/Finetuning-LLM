from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "product_id",
        F.quarter("last_restock").alias("last_restock_quarter"),
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
    )
    .agg(
        F.max("last_restock").alias("last_last_restock"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") > 2)
    .drop("cnt")
)
