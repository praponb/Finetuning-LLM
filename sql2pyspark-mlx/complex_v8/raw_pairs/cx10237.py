from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("status").orderBy(F.col("start_date").desc(), F.col("id").desc())
w_dedup_2 = Window.partitionBy("holder_region", "holder_id").orderBy("start_date", "id")
w_holder_region = Window.partitionBy("holder_region")
w_all_by_start_date_ties = Window.orderBy(F.col("start_date").desc())
w_all_by_start_date = Window.orderBy(F.col("start_date").desc(), "id")

cleaned = (
    df_policies
    .filter(F.col("product").isin("HEALTH", "AUTO", "LIFE"))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        F.when(F.col("premium") >= 3521.8, "HIGH").when(F.col("premium") >= 2096.6, "MEDIUM").otherwise("LOW").alias("premium_tier"),
    )
)

combined_details_holder = (
    cleaned.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.status",
        "s.premium_tier",
        F.col("p.region").alias("holder_region"),
    )
)

unique_rows = (
    combined_details_holder
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

enriched = (
    unique_rows
    .filter(F.col("holder_region") == "South")
    .withColumn("start_date_month", F.month(F.col("start_date")))
)

latest_per_key = (
    enriched
    .withColumn("row_num", F.row_number().over(w_dedup_2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

metrics = (
    latest_per_key
    .select(
        "*",
        F.round(F.avg("start_date_month").over(w_holder_region), 2).alias("holder_region_avg_start_date_month"),
        F.count(F.lit(1)).over(w_holder_region).alias("rows_in_holder_region"),
        F.rank().over(w_all_by_start_date_ties).alias("start_date_rank"),
        F.round(F.percent_rank().over(w_all_by_start_date), 3).alias("start_date_percent_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        "holder_region",
        "start_date_month",
        "rows_in_holder_region",
        "start_date_rank",
        "start_date_percent_rank",
    )
    .orderBy(F.col("id").asc_nulls_last(), F.col("holder_id").asc_nulls_last())
)
