from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_status_by_premium = Window.partitionBy("holder_id", "status").orderBy(F.col("premium").desc())
w_holder_id = Window.partitionBy("holder_id")
w_holder_id_status_by_premium_2 = Window.partitionBy("holder_id", "status").orderBy(F.col("premium").desc(), "id")
w_holder_id_status_by_premium_3 = Window.partitionBy("holder_id", "status").orderBy(F.col("premium").desc(), "id").rowsBetween(-1, -1)

prepared_policys = (
    df_policies
    .filter(F.col("start_date") < F.lit("2025-02-14").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.date_add(F.col("start_date"), 30).alias("start_date_plus_30d"),
    )
)

with_windows = (
    prepared_policys
    .select(
        "id",
        "product",
        "premium",
        "end_date",
        "status",
        F.round(F.cume_dist().over(w_holder_id_status_by_premium), 3).alias("premium_cume_dist"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id), 2).alias("premium_vs_holder_id_avg"),
        F.ntile(10).over(w_holder_id_status_by_premium_2).alias("premium_decile"),
        F.max("premium").over(w_holder_id_status_by_premium_3).alias("premium_max_1_prior"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["product", "status", "premium_decile"], ["product"], ["status"]],
        "product", "status", "premium_decile",
    )
    .agg(
        F.min("end_date").alias("first_end_date"),
        F.count("*").alias("row_count"),
    )
    .select(
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "premium_decile",
        "first_end_date",
        "row_count",
    )
    .orderBy(
        F.col("product").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
        F.col("premium_decile").asc_nulls_last(),
    )
)
