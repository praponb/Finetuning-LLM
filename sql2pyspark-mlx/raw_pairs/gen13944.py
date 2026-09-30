from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_last(), "id")

deduped = (
    df_suppliers
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .groupBy("country")
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("rating_per_london"),
        F.max("rating").alias("max_rating"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("rating"))).alias("bangkok_total_rating"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
    )
    .filter(F.col("max_rating") > 0)
)
