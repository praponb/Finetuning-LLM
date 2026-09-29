from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc_nulls_last(), "id")

cleaned = (
    df_stores
    .filter((F.col("region").eqNullSafe(F.lit("Central"))) & (F.col("opened_date") >= F.to_date(F.lit("2023-06-18"))))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.trim("city").alias("clean_city"),
        F.coalesce("region", F.lit("NONE")).alias("resolved_region"),
    )
)

result = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
