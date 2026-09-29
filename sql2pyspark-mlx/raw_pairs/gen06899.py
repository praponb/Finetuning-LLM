from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())

latest_match = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.region",
        "s.opened_date",
        F.col("c.revenue").alias("store_sale_revenue"),
        F.col("c.units"),
    )
)

result = (
    latest_match
    .groupBy("region")
    .agg(
        F.count(F.when(F.col("region") == "West", 1)).alias("west_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_count"),
    )
)
