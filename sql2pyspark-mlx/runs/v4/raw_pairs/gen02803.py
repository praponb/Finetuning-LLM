from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-06-16").cast("date"))
    .withColumn("quantity_num", F.col("quantity").cast("string"))
)

result = (
    base
    .groupBy(F.date_trunc("YEAR", "last_restock").alias("year_start"), "product_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.sum("quantity").alias("total_quantity"),
    )
    .orderBy(F.col("quantity_stddev").desc())
)
