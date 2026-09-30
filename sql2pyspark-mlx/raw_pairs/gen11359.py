from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~(F.col("product_id").isin([2, 3, 6, 8]) & F.col("product_id").isin([1, 3, 4, 8])))
    .groupBy(
        F.year("last_restock").alias("last_restock_year"),
        F.date_trunc("MONTH", "last_restock").alias("last_restock_month"),
    )
    .agg(F.min("last_restock").alias("first_last_restock"))
)
