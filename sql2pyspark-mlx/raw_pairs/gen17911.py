from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc_nulls_first())

cleaned = (
    df_stores
    .filter(F.col("region").isNotNull() & ~F.col("region").isin(["South", "Central"]))
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        F.concat_ws(",", "city", "store_name").alias("city_store_name_key"),
        F.substring("city", 1, 4).alias("city_prefix"),
    )
)

result = (
    cleaned
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)
