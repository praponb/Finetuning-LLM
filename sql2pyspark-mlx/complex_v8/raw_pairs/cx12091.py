from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_premium_moving_1 = Window.orderBy("premium", "id").rowsBetween(-1, Window.currentRow)
w_all_by_premium = Window.orderBy("premium", "id")
w_holder_id_status = Window.partitionBy("holder_id", "status")
w_latest = Window.partitionBy("holder_id", "product").orderBy(F.col("premium_max_2").desc(), F.col("id").desc())
w_premium_decile_by_end_date = Window.partitionBy("premium_decile").orderBy("end_date", "id")
w_premium_decile_by_end_date_f = Window.partitionBy("premium_decile").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id = Window.partitionBy("holder_id")

filtered_policys = (
    df_policies
    .filter((F.col("start_date") < F.lit("2025-02-13").cast("date")) & (F.col("status").isin("CANCELLED", "ACTIVE", "EXPIRED")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
    )
)

metrics = (
    filtered_policys
    .select(
        "*",
        F.max("premium").over(w_all_by_premium_moving_1).alias("premium_max_2"),
        F.ntile(10).over(w_all_by_premium).alias("premium_decile"),
        F.min("premium").over(w_holder_id_status).alias("holder_id_status_min_premium"),
    )
)

deduped = (
    metrics
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_trends = (
    deduped
    .select(
        "id",
        "holder_id",
        "end_date",
        "status",
        "holder_id_status_min_premium",
        F.ntile(5).over(w_premium_decile_by_end_date).alias("end_date_quintile"),
        F.last("end_date").over(w_premium_decile_by_end_date_f).alias("last_end_date"),
        F.max("premium").over(w_holder_id).alias("holder_id_max_premium"),
    )
)

result = (
    with_trends
    .select("holder_id_max_premium", "id")
    .distinct()
    .orderBy("id")
)
