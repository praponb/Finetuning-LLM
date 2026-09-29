from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id_by_voltage = Window.partitionBy("quality", "site_id").orderBy("voltage", "id")
w_site_id = Window.partitionBy("site_id")
w_distinct_quality_in_site_id = Window.partitionBy("distinct_quality_in_site_id").orderBy("minutes_since_prev_read_at", "id")
w_quality_distinct_quality_in = Window.partitionBy("quality", "distinct_quality_in_site_id")

valid_readings = (
    df_meter_readings
    .filter(F.col("kwh") < 79.7)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

windowed = (
    valid_readings
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_quality_site_id_by_voltage))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.size(F.collect_set("quality").over(w_site_id)).alias("distinct_quality_in_site_id"),
        F.sum("voltage").over(w_site_id).alias("site_id_sum_voltage"),
    )
)

metrics = (
    windowed
    .select(
        "*",
        F.rank().over(w_distinct_quality_in_site_id).alias("minutes_since_prev_read_at_rank"),
        F.lead("site_id_sum_voltage").over(w_distinct_quality_in_site_id).alias("next_site_id_sum_voltage"),
        (F.col("distinct_quality_in_site_id") - F.lag("distinct_quality_in_site_id").over(w_distinct_quality_in_site_id)).alias("distinct_quality_in_site_id_change"),
    )
)

result = (
    metrics
    .groupBy("read_at", "minutes_since_prev_read_at_rank")
    .pivot("quality", ["BAD", "EST", "OK"])
    .agg(F.count("voltage").alias("count_v"), F.max("voltage").alias("max_v"))
)
