from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("temp_c", "id")
w4 = Window.orderBy("device_code_max_temp_c", "id")
w5 = Window.orderBy("temp_c", "id")
w_hist = Window.orderBy("row_seq", "id").rowsBetween(-3, Window.currentRow)

scored = (
    df_device_readings
    .filter(F.col("device_code") != "DV-04")
    .select(
        "id",
        "temp_c",
        F.max("temp_c").over(w).alias("device_code_max_temp_c"),
        F.sum("status_bits").over(w2).alias("running_status_bits"),
        F.lag("humidity", 1).over(w3).alias("prev_humidity"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.lead("temp_c").over(w4).alias("next_temp_c"),
        F.lag("temp_c").over(w4).alias("prev_temp_c"),
        F.lag("running_status_bits").over(w4).alias("prev_running_status_bits"),
        F.lead("running_status_bits").over(w4).alias("next_running_status_bits"),
        F.row_number().over(w4).alias("row_seq"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.count("prev_humidity").over(w_hist).alias("last4_count_prev_humidity"),
        F.max("prev_humidity").over(w_hist).alias("last4_max_prev_humidity"),
        F.min("prev_humidity").over(w_hist).alias("last4_min_prev_humidity"),
    )
)

valid = (
    metrics
    .filter(F.col("last4_max_prev_humidity") >= 76)
    .select(
        "id",
        "device_code_max_temp_c",
        "running_status_bits",
        "prev_humidity",
        "next_temp_c",
        "prev_running_status_bits",
        "row_seq",
        "event_seq",
        "last4_count_prev_humidity",
        "last4_min_prev_humidity",
        F.when(F.col("temp_c") >= 36.6, "High").when(F.col("temp_c") >= 18.8, "Medium").otherwise("Low").alias("temp_c_level"),
    )
)

result = (
    valid
    .select("id", "next_temp_c", "last4_min_prev_humidity")
    .unpivot(
        ids=["id"],
        values=["next_temp_c", "last4_min_prev_humidity"],
        variableColumnName="measure",
        valueColumnName="value",
    )
)
