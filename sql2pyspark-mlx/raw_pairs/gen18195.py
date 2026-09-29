from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("region").orderBy(F.col("opened_date").desc(), "id")

scored = (
    df_stores
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.max("sqft").over(w).alias("last4_max_sqft"),
        F.count("sqft").over(w).alias("last4_count_sqft"),
        F.avg("sqft").over(w).alias("last4_avg_sqft"),
        F.min("sqft").over(w).alias("last4_min_sqft"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

result = (
    scored
    .groupBy("region")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.percentile_approx("sqft", 0.5).alias("sqft_p50"),
    )
    .filter(F.col("sqft_p50") >= 0)
    .orderBy(F.col("sqft_p50").asc_nulls_last())
)
