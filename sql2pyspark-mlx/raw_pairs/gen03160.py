from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("rating", "id")
w2 = Window.partitionBy("country", "city")

enriched = (
    df_suppliers
    .withColumn("prev_rating", F.lag("rating").over(w))
    .withColumn("next_rating", F.lead("rating").over(w))
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("country_min_rating", F.min("rating").over(w2))
)

result = (
    enriched
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("city") == "New York", F.col("prev_rating"))).alias("new_york_total_prev_rating"),
        F.sum(F.when(F.col("country") == "JP", F.col("country_min_rating"))).alias("jp_total_country_min_rating"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") > 1)
)
