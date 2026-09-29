from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("rating").desc_nulls_last(), "id")

ranked = (
    df_suppliers
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("rating") >= 3.6, "LARGE").otherwise("SMALL").alias("rating_tier"),
        "city",
    )
    .agg(
        F.countDistinct("country").alias("distinct_countries"),
        F.count_if(F.col("city") == "Bangkok").alias("bangkok_events"),
        F.max("rating").alias("max_rating"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_rows"),
    )
)
