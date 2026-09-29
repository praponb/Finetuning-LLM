from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date", "id")

ranked = (
    df_shipments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "order_id", "carrier", "shipped_date", "delivered_date", "status")
)

result = (
    ranked
    .groupBy(F.date_trunc("YEAR", "delivered_date").alias("year_start"))
    .agg(
        F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_rows"),
        F.countDistinct("status").alias("distinct_status"),
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
    .orderBy("distinct_status")
)
