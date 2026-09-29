from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.orderBy("rating", "id")
w3 = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_last())
w4 = Window.partitionBy("city").orderBy("supplier_name_length", "id")
w5 = Window.partitionBy("city").orderBy(F.col("supplier_name_length_bucket").desc(), "id")
w6 = Window.partitionBy("city").orderBy("supplier_name_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("country") != "UK"))
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.length("supplier_name").alias("supplier_name_length"),
        F.coalesce("city", F.lit("DIRECT")).alias("resolved_city"),
    )
)

latest = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    latest
    .withColumn("prev_supplier_name_length", F.lag("supplier_name_length", 1).over(w2))
    .withColumn("rating_percent_rank", F.percent_rank().over(w3))
    .withColumn("supplier_name_length_bucket", F.ntile(5).over(w4))
)

scored = (
    enriched
    .withColumn("supplier_name_length_bucket_2", F.ntile(3).over(w4))
    .withColumn("supplier_name_length_bucket_bucket", F.ntile(10).over(w5))
    .withColumn("running_rating_percent_rank", F.sum("rating_percent_rank").over(w6))
)

result = (
    scored
    .groupBy(
        F.when(F.col("rating_percent_rank") >= 0.73, "PREMIUM").otherwise("STANDARD").alias("rating_percent_rank_tier"),
    )
    .agg(
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_rows"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("rating_per_london"),
        F.max("running_rating_percent_rank").alias("max_running_rating_percent_rank"),
        F.sum(F.when(F.col("city") == "Tokyo", F.col("supplier_name_length_bucket_2"))).alias("tokyo_total_supplier_name_length_bucket_2"),
    )
    .filter(F.col("tokyo_total_supplier_name_length_bucket_2") > 0)
)
