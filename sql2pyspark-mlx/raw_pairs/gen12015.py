from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("region").orderBy(F.col("sqft_whole").desc_nulls_first(), "id")
w3 = Window.partitionBy("region").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("region").orderBy("rank_in_group", "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_stores
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-09-20"))) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        F.date_format("opened_date", "MMM yyyy").alias("opened_date_label"),
        F.floor("sqft").alias("sqft_whole"),
    )
)

deduped = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

latest = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

kept = (
    latest
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") <= F.col("s.opened_date")), "left_semi")
)

latest_stores = (
    kept
    .withColumn("rank_in_group_2", F.row_number().over(w2))
    .filter(F.col("rank_in_group_2") == 1)
    .drop("rank_in_group_2")
)

metrics = (
    latest_stores
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        "opened_date_label",
        "sqft_whole",
        "row_num",
        F.count("sqft_whole").over(w3).alias("running_count_sqft_whole"),
        F.max("sqft_whole").over(w3).alias("running_max_sqft_whole"),
        F.min("sqft_whole").over(w3).alias("running_min_sqft_whole"),
        F.avg("sqft_whole").over(w4).alias("sqft_whole_avg_2"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") >= F.col("s.opened_date")), "left_anti")
)

report = (
    eligible
    .select(
        "id",
        "region",
        "sqft",
        "opened_date_label",
        "sqft_whole",
        "row_num",
        "running_count_sqft_whole",
        "running_max_sqft_whole",
        "running_min_sqft_whole",
    )
)

result = (
    report
    .select("id", "region", "sqft", "row_num", "running_min_sqft_whole")
    .orderBy("running_min_sqft_whole")
)
