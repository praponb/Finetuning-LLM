from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("units", "id")

latest = (
    df_store_sales
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    latest
    .groupBy(
        F.when(F.col("units") >= 42, "PREMIUM").otherwise("STANDARD").alias("units_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("units", 0.95).alias("units_p95"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("distinct_ids") >= 1)
)
