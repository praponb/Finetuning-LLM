from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_last(), "id")

ranked = (
    df_suppliers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "supplier_name", "city", "rating")
)

result = (
    ranked
    .groupBy(
        F.when(F.col("rating") >= 3.5, "LARGE").otherwise("SMALL").alias("rating_tier"),
    )
    .agg(
        F.count_if(F.col("city") == "Chiang Mai").alias("chiang_mai_events"),
        F.countDistinct("city").alias("distinct_cities"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("rating_tier").asc_nulls_last())
)
