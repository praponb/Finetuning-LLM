from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc_nulls_first(), "id")

deduped = (
    df_store_sales
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .groupBy(
        "product_id",
        F.date_trunc("QUARTER", "sale_date").alias("quarter_start"),
        F.when(F.col("revenue") >= 3200, "HIGH").otherwise("LOW").alias("revenue_tier"),
    )
    .agg(
        F.percentile_approx("units", 0.9).alias("units_p90"),
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
    .orderBy(F.col("quarter_start").desc())
)
