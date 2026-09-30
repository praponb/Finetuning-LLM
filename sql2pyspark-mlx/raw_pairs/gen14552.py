from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(143, 160))
    .groupBy("product_id", F.year("last_restock").alias("last_restock_year"))
    .agg(
        F.max("quantity").alias("max_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .filter(F.col("quantity_range") > 57)
    .orderBy(F.col("last_restock_year").desc())
)
