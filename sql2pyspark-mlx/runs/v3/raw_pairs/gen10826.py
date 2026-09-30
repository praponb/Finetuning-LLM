from pyspark.sql import functions as F

result = (
    df_order_items
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("max_unit_price").desc())
)
