from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_first(), "id")
w2 = Window.partitionBy("country").orderBy(F.col("rating").asc_nulls_first(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select("*", F.length("country").alias("country_length"))
)

deduped = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "supplier_name", "country", "city", "rating")
)

deduped_suppliers = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "supplier_name", "city", "rating")
)

latest_match = (
    deduped_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost").alias("product_cost"))
)

result = (
    latest_match
    .withColumn("rating_tier", F.when(F.col("rating") >= 4.0, "PREMIUM").otherwise("STANDARD"))
    .groupingSets([["city", "rating_tier"], ["city"]], "city", "rating_tier")
    .agg(
        F.avg("rating").alias("avg_rating"),
        F.stddev_samp("rating").alias("rating_stddev"),
        F.count_if(F.col("city") == "Bangkok").alias("bangkok_events"),
    )
    .orderBy("city")
)
