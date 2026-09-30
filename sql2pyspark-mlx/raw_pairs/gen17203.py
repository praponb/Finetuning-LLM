from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc(), "id")

filtered = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-05-22").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.regexp_replace("store_name", " ", "-").alias("store_name_slug"),
        F.initcap(F.trim("city")).alias("city_title"),
    )
)

result = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "city", "region", "store_name_slug", "city_title")
)
