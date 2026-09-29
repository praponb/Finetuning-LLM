from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")

base = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-09-06").cast("date")) & F.col("region").isNotNull() & F.col("city").isin(["Bangkok", "Tokyo"]))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        F.trim("city").alias("clean_city"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_filled"),
    )
)

result = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
