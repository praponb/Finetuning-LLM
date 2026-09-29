from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").asc_nulls_first(), "id")

deduped = (
    df_store_sales
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("activity_month"), "product_id")
    .agg(
        F.percentile_approx("units", [0.25, 0.5, 0.75]).alias("units_quartiles"),
        F.stddev_samp("revenue").alias("revenue_stddev"),
    )
)
