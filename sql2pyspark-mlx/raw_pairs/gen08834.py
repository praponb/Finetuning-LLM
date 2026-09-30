from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date_month_end").asc_nulls_first(), "id")

valid = (
    df_stores
    .filter(F.col("opened_date") >= F.lit("2023-05-11").cast("date"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.when(F.col("city").isin(["London", "Berlin"]), "Group A").otherwise("Other").alias("city_group"),
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.coalesce("region", F.lit("NONE")).alias("region_filled"),
    )
)

result = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "store_name", "region", "opened_date", "city_group", "region_filled")
)
