from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("city").orderBy("opened_on", "id")
w_dedup_2 = Window.partitionBy("city").orderBy(F.col("opened_on").desc(), F.col("id").desc())

prepared = (
    df_warehouses
    .filter(F.lower(F.col("wh_code")).like("%ku%"))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.concat_ws(" / ", F.col("city"), F.col("wh_code")).alias("city_wh_code_label"),
    )
)

latest = (
    prepared
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

latest_per_key = (
    latest
    .withColumn("recency_rank", F.row_number().over(w_dedup_2))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key
    .select(
        "id",
        "capacity",
        "opened_on",
        "city_wh_code_label",
        F.abs(F.col("capacity") - 11005).alias("capacity_distance"),
        F.when(F.col("city").isin("Kuala Lumpur", "Bangkok"), "group_a").when(F.col("city") == "Singapore", "group_b").otherwise("other").alias("city_group"),
    )
    .orderBy(F.col("id").asc())
    .limit(5)
)
