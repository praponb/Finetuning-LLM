from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code")

enriched = (
    df_device_readings
    .select(
        "*",
        F.sum("temp_c").over(w_grp).alias("running_sum_temp_c"),
        F.max("temp_c").over(w_grp).alias("running_max_temp_c"),
        F.count("temp_c").over(w_grp).alias("running_count_temp_c"),
        F.round((F.col("temp_c") / F.sum("temp_c").over(w2)), 4).alias("temp_c_share"),
    )
)

result = (
    enriched
    .groupBy(F.date_trunc("hour", "reading_ts").alias("hour_window"))
    .agg(
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        F.round(F.sum("running_max_temp_c") / F.nullif(F.count(F.when(F.col("device_code") == "DV-03", 1)), F.lit(0)), 2).alias("running_max_temp_c_per_dv_03"),
    )
    .orderBy("hour_window")
)
