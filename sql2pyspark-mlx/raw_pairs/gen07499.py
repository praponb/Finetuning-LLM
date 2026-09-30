from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id")

latest = (
    df_stores
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .groupBy(
        F.date_trunc("QUARTER", "opened_date").alias("quarter_start"),
        F.when(F.col("sqft") >= 17078, "HIGH").otherwise("LOW").alias("sqft_tier"),
    )
    .agg(
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("sqft", 0.5).alias("sqft_p50"),
        F.max("sqft").alias("max_sqft"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy(F.col("bangkok_rows").desc_nulls_last())
)
