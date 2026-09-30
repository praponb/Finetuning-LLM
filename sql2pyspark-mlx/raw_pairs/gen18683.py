from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())

with_latest = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units"), F.col("c.revenue").alias("store_sale_revenue"))
)

result = (
    with_latest
    .groupBy(
        "city",
        F.when(F.col("store_sale_revenue") >= 3900, "HIGH").otherwise("LOW").alias("store_sale_revenue_tier"),
        "region",
    )
    .agg(
        F.count(F.when(F.col("region") == "East", 1)).alias("east_count"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
    )
    .orderBy(F.col("city").asc_nulls_last())
)
