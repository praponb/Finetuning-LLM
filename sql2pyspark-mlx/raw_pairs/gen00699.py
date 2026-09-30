from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.date_trunc("MONTH", "last_restock").alias("activity_month"))
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.sum("quantity").alias("total_quantity"),
    )
    .filter(F.col("quantity_bit_and") < 1000)
    .orderBy("quantity_bit_and")
)
