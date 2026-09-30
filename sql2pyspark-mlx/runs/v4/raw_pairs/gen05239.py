from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy("city")

metrics = (
    df_suppliers
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w2)), 4))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("rating") >= 4.1, "LARGE").otherwise("SMALL").alias("rating_tier"),
        F.when(F.col("row_seq") >= 8, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
        "city",
    )
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("rating_share")).otherwise(0)).alias("chiang_mai_rating_share"),
        F.countDistinct("city").alias("distinct_cities"),
        F.avg(F.when(F.col("country") == "JP", F.col("rating"))).alias("jp_avg_rating"),
        F.round(F.avg("rating_share"), 2).alias("avg_rating_share"),
        F.max(F.when(F.col("city") == "Bangkok", F.col("rating_share"))).alias("bangkok_max_rating_share"),
    )
)
