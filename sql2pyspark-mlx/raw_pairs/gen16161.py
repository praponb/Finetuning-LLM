from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue", "id")

deduped = (
    df_store_sales
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    deduped
    .groupBy(F.date_trunc("QUARTER", "sale_date").alias("quarter_start"), "product_id")
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.percentile_approx("units", 0.5).alias("units_p50"),
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
    )
    .orderBy(F.col("units_p50").desc())
)
