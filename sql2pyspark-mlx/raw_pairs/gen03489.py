from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("region").orderBy(F.col("sqft").desc_nulls_first(), "id")
w3 = Window.partitionBy("city", "region").orderBy("sqft", "id")
w4 = Window.partitionBy("region")
w5 = Window.partitionBy("city").orderBy(F.col("row_num").desc())
w6 = Window.partitionBy("city").orderBy("seq_no", "id")
w7 = Window.partitionBy("city").orderBy(F.col("seq_no").desc(), "id")

latest = (
    df_stores
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

filtered = (
    latest
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-03-07"))) & F.col("region").isin(["East", "West", "Central"]))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "dedup_rank",
        F.lower("store_name").alias("store_name_lower"),
    )
)

deduped = (
    filtered
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    deduped
    .select(
        "id",
        "city",
        "dedup_rank",
        "row_num",
        F.row_number().over(w3).alias("seq_no"),
        F.round((F.col("sqft") / F.sum("sqft").over(w4)), 4).alias("sqft_share"),
    )
)

ranked = (
    with_windows
    .withColumn("rank_in_group", F.rank().over(w5))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "city", "row_num", "seq_no")
)

valid = (
    ranked
    .filter(F.col("city").isin(["Singapore", "New York"]))
    .select("*", F.initcap(F.trim("city")).alias("city_title"))
)

scored = (
    valid
    .filter(F.col("seq_no") != 6)
    .select(
        "*",
        F.ntile(5).over(w6).alias("seq_no_bucket"),
        F.row_number().over(w7).alias("row_seq"),
    )
)

result = (
    scored
    .groupBy("seq_no")
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count_if(F.col("city") == "Bangkok").alias("bangkok_events"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_rows"),
    )
    .orderBy(F.col("num_cities").desc_nulls_first())
)
