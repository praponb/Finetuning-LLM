from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 8, "LARGE").otherwise("SMALL").alias("quantity_tier"),
    )
    .agg(
        F.corr("unit_price", "quantity").alias("corr_unit_price_quantity"),
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("corr_unit_price_quantity") >= 0)
)
