from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.bit_and("quantity").alias("quantity_bit_and_2"),
        F.bit_and("quantity").alias("quantity_bit_and_3"),
    )
    .orderBy(F.col("quantity_bit_and_3").desc())
)
