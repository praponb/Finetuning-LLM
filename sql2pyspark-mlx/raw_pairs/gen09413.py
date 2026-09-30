from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("start_date").desc_nulls_first(), "id")

latest = (
    df_subscriptions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "end_date")
)

result = (
    latest
    .withColumn("year_start", F.date_trunc("YEAR", "end_date"))
    .withColumn("week_start", F.date_trunc("WEEK", "end_date"))
    .groupingSets(
        [["year_start", "week_start"], ["year_start"], []],
        "year_start",
        "week_start",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.countDistinct("id").alias("distinct_ids_3"),
    )
    .orderBy("cnt")
)
