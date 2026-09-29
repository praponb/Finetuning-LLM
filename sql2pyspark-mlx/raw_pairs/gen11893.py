from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc_nulls_first(), "id")

latest = (
    df_products
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "product_name", "stock_qty", "supplier_id")
)

result = (
    latest
    .groupBy(
        F.when(F.col("stock_qty") >= 381, "PREMIUM").otherwise("STANDARD").alias("stock_qty_tier"),
    )
    .agg(
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
        F.count("*").alias("cnt"),
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty_2"),
        F.percentile_approx("stock_qty", [0.25, 0.5, 0.75]).alias("stock_qty_quartiles"),
    )
    .orderBy(F.col("cnt").desc())
)
