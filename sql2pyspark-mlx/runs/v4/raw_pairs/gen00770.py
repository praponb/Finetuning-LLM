from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating_scaled").desc())
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("city", "country").orderBy("rating")
w4 = Window.orderBy("rating", "id")
w5 = Window.partitionBy("city").orderBy("rating_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_suppliers
    .filter(F.col("rating") <= 3.4)
    .withColumn("rating_scaled", F.round((F.col("rating") * 1.07), 2))
    .withColumn("supplier_name_title", F.initcap(F.trim("supplier_name")))
)

ranked = (
    cleaned
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

with_windows = (
    ranked
    .select(
        "*",
        F.sum("rating_scaled").over(w2).alias("city_sum_rating_scaled"),
        F.cume_dist().over(w3).alias("rating_cume_dist"),
        F.row_number().over(w4).alias("row_seq"),
        F.last("city", ignorenulls=True).over(w5).alias("last_known_city"),
    )
)

valid = (
    with_windows
    .filter(F.col("city").isNotNull())
    .select("*", F.round((F.col("rn") * 1.15), 2).alias("adjusted_rn"))
)

result = (
    valid
    .groupBy(
        F.when(F.col("rating_scaled") >= 76, "LARGE").otherwise("SMALL").alias("rating_scaled_tier"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", F.col("adjusted_rn"))).alias("bangkok_total_adjusted_rn"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.percentile_approx("row_seq", 0.9).alias("row_seq_p90"),
        F.percentile_approx("city_sum_rating_scaled", 0.9).alias("city_sum_rating_scaled_p90"),
    )
    .orderBy(F.col("row_seq_p90").asc_nulls_last())
)
