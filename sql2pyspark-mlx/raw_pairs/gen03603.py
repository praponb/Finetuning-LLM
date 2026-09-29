from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.bit_and("quantity").alias("quantity_bit_and"),
    )
    .orderBy(F.col("quantity_bit_and").desc_nulls_first())
)
