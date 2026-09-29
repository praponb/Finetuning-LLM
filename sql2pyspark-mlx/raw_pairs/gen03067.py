from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("rating").desc_nulls_last(), "id")

ranked = (
    df_suppliers
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("rating") >= 4.3, "LARGE").otherwise("SMALL").alias("rating_tier"),
    )
    .agg(
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_rows"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles_2"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("city") == "New York", 1)), F.lit(0)), 2).alias("rating_per_new_york"),
    )
    .orderBy(F.col("cnt").desc_nulls_first())
)
