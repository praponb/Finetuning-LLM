from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")

deduped = (
    df_sessions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped
    .withColumn("quarter_start", F.date_trunc("quarter", "started_at"))
    .withColumn("year_start", F.date_trunc("year", "started_at"))
    .groupingSets(
        [["quarter_start", "year_start"], ["quarter_start"], []],
        "quarter_start",
        "year_start",
    )
    .agg(
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_count"),
        F.size(F.collect_set("device")).alias("num_devices"),
    )
)
