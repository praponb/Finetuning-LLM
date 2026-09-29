from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("units").desc_nulls_first(), "id")
w2 = Window.partitionBy("product_id").orderBy("units")
w3 = Window.partitionBy("region", "store_id").orderBy(F.col("revenue").asc_nulls_last(), "id")
w4 = Window.partitionBy("city")
w5 = Window.partitionBy("city").orderBy(F.col("revenue").desc())

ranked = (
    df_store_sales
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked_store_sales = (
    ranked
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

enriched = (
    ranked_store_sales
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.revenue",
        F.col("s.region"),
        F.col("s.opened_date"),
        F.col("s.city"),
    )
)

deduped = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select("id", "revenue", "region", "opened_date", "city")
)

result = (
    deduped
    .select(
        "id",
        "revenue",
        "city",
        F.stddev_samp("revenue").over(w4).alias("city_stddev_samp_revenue"),
        F.percent_rank().over(w5).alias("revenue_percent_rank"),
    )
)
