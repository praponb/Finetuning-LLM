from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc_nulls_last(), "id")
w_grp = Window.partitionBy("device_code").orderBy("seq_no_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("temp_c", "id")

enriched = (
    df_device_readings
    .select(
        "id",
        "device_code",
        "temp_c",
        "status_bits",
        F.lag("temp_c").over(w).alias("prev_temp_c"),
        F.lag("status_bits").over(w).alias("prev_status_bits"),
        F.row_number().over(w).alias("seq_no"),
    )
)

base = (
    enriched
    .filter(F.col("temp_c") <= 32)
    .select(
        "id",
        "device_code",
        "temp_c",
        "prev_temp_c",
        "prev_status_bits",
        F.coalesce("seq_no", F.lit(0)).alias("seq_no_filled"),
        F.greatest("prev_temp_c", "prev_status_bits").alias("greatest_prev_temp_c_prev_status_bits"),
    )
)

ranked = (
    base
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

scored = (
    ranked
    .filter(F.col("seq_no_filled") <= 48)
    .withColumn("running_min_seq_no_filled", F.min("seq_no_filled").over(w_grp))
    .withColumn("running_count_seq_no_filled", F.count("seq_no_filled").over(w_grp))
    .withColumn("running_max_seq_no_filled", F.max("seq_no_filled").over(w_grp))
    .withColumn("prev_prev_temp_c", F.lag("prev_temp_c", 1, 0).over(w4))
)

result = (
    scored
    .withColumn("running_count_seq_no_filled_tier", F.when(F.col("running_count_seq_no_filled") >= 13, "HIGH").otherwise("LOW"))
    .cube("device_code", "running_count_seq_no_filled_tier")
    .agg(
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list_2"),
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events"),
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events_2"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("running_count_seq_no_filled_tier").desc())
)
