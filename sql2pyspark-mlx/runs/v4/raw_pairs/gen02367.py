from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id")

enriched = (
    df_store_sales
    .withColumn("cumulative_units", F.sum("units").over(w))
    .withColumn("revenue_share", F.round((F.col("revenue") / F.sum("revenue").over(w2)), 4))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("revenue_share") >= 0.62, "PREMIUM").otherwise("STANDARD").alias("revenue_share_tier"),
        F.date_trunc("WEEK", "sale_date").alias("week_start"),
    )
    .agg(
        F.corr("units", "revenue").alias("corr_units_revenue"),
        F.count("*").alias("event_count"),
        F.corr("revenue", "units").alias("corr_revenue_units"),
    )
    .orderBy(F.col("revenue_share_tier").desc_nulls_last())
)
