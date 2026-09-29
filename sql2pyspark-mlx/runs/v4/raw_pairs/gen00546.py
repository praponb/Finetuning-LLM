from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_first())

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.round("rating", 2).alias("rating_rounded"),
        F.substring("country", 1, 3).alias("country_prefix"),
    )
)

result = (
    valid
    .withColumn("city_min_rating", F.min("rating").over(w))
    .withColumn("rating_percent_rank", F.percent_rank().over(w2))
)
