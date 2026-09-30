from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(-3, Window.currentRow)

enriched = (
    df_store_sales
    .withColumn("last4_avg_revenue", F.avg("revenue").over(w))
    .withColumn("last4_max_revenue", F.max("revenue").over(w))
    .withColumn("last4_min_revenue", F.min("revenue").over(w))
)

result = (
    enriched
    .select(
        "id",
        "sale_date",
        "units",
        "revenue",
        "last4_avg_revenue",
        "last4_min_revenue",
    )
    .orderBy(F.col("id").asc_nulls_first())
)
