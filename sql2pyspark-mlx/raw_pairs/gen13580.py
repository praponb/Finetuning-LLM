from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity").asc_nulls_first(), "id")

latest = (
    df_order_items
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .groupBy(
        "product_id",
        F.when(F.col("unit_price") >= 390, "HIGH").otherwise("LOW").alias("unit_price_tier"),
    )
    .agg(
        F.corr("unit_price", "quantity").alias("corr_unit_price_quantity"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
    )
    .orderBy(F.col("corr_unit_price_quantity").desc())
)
