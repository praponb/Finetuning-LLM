from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("region", "city").orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("region").orderBy(F.col("last4_min_sqft").asc_nulls_first())
w3 = Window.partitionBy("region").orderBy(F.col("opened_date").desc())

cleaned = (
    df_stores
    .filter(F.col("sqft") >= 6695)
    .withColumn("store_name_city", F.concat("store_name", F.lit(" "), "city"))
)

metrics = (
    cleaned
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        F.min("sqft").over(w_running).alias("last4_min_sqft"),
        F.count("sqft").over(w_running).alias("last4_count_sqft"),
        F.max("sqft").over(w_running).alias("last4_max_sqft"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.revenue") >= 1300), "left_semi")
)

ranked = (
    not_flagged
    .withColumn("row_num", F.rank().over(w2))
    .filter(F.col("row_num") == 1)
    .select("id", "store_name", "region", "opened_date")
)

ranked_stores = (
    ranked
    .withColumn("rn", F.rank().over(w3))
    .filter(F.col("rn") == 1)
)

result = (
    ranked_stores
    .rollup("region", F.date_trunc("QUARTER", "opened_date").alias("quarter_start"))
    .agg(
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.count(F.when(F.col("region") == "North", 1)).alias("north_rows"),
        F.count(F.when(F.col("region") == "North", 1)).alias("north_count"),
        F.count_if(F.col("region") == "Central").alias("central_events"),
        F.sort_array(F.collect_set("region")).alias("region_list_2"),
    )
    .orderBy(F.col("region").asc_nulls_last())
)
