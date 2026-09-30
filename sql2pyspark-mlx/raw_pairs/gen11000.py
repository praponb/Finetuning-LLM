from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy(F.col("opened_date").asc_nulls_last(), "id")

cleaned = (
    df_stores
    .filter(F.col("region").isNotNull() & F.col("sqft").between(4471, 7314))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.trim("city").alias("clean_city"),
        F.regexp_replace("store_name", " ", "_").alias("store_name_slug"),
    )
)

result = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
