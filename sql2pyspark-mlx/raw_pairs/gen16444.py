from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("activity_month").orderBy(F.col("units_bit_and").desc())

result = (
    df_store_sales
    .groupBy(
        F.date_trunc("MONTH", "sale_date").alias("activity_month"),
        F.when(F.col("revenue") >= 3600, "HIGH").otherwise("LOW").alias("revenue_tier"),
        F.date_trunc("QUARTER", "sale_date").alias("quarter_start"),
    )
    .agg(
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.bit_and("units").alias("units_bit_and"),
        F.percentile_approx("units", [0.25, 0.5, 0.75]).alias("units_quartiles"),
    )
    .withColumn("units_bit_and_dense_rank", F.dense_rank().over(w))
)
