from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date", "id")

ranked = (
    df_shipments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "carrier", "shipped_date", "status")
)

result = (
    ranked
    .groupBy(F.date_trunc("quarter", "shipped_date").alias("quarter_start"))
    .agg(
        F.countDistinct("carrier").alias("distinct_carriers"),
        F.count("*").alias("event_count"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("num_status").desc_nulls_first())
)
