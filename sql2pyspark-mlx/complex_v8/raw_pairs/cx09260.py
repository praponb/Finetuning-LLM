from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_premium_tier_by_en = Window.partitionBy("holder_id", "premium_tier").orderBy(F.col("end_date").asc_nulls_last(), "id")
w_premium_tier = Window.partitionBy("premium_tier")
w_holder_id_premium_tier_by_en_2 = Window.partitionBy("holder_id", "premium_tier").orderBy(F.col("end_date").asc_nulls_last())
w_premium_tier_status = Window.partitionBy("premium_tier", "status")
w_holder_id_by_distinct_holder = Window.partitionBy("holder_id").orderBy("distinct_holder_id_in_premium_tier", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_by_distinct_holder_2 = Window.partitionBy("holder_id").orderBy(F.col("distinct_holder_id_in_premium_tier").desc(), "id").rowsBetween(-6, -1)
w_holder_id_by_distinct_holder_3 = Window.partitionBy("holder_id").orderBy(F.col("distinct_holder_id_in_premium_tier").desc(), "id")
w_latest = Window.partitionBy("premium_tier").orderBy(F.col("rows_in_premium_tier_status").desc(), F.col("id").desc())

recent = (
    df_policies
    .filter((F.col("premium") > 2289.0) & (F.col("product").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("premium") >= 4417.0, "HIGH").when(F.col("premium") >= 2289.0, "MEDIUM").otherwise("LOW").alias("premium_tier"),
    )
)

with_windows = (
    recent
    .select(
        "*",
        F.lead("start_date").over(w_holder_id_premium_tier_by_en).alias("next_start_date"),
        F.min("premium").over(w_premium_tier).alias("premium_tier_min_premium"),
        F.size(F.collect_set("holder_id").over(w_premium_tier)).alias("distinct_holder_id_in_premium_tier"),
        F.rank().over(w_holder_id_premium_tier_by_en_2).alias("end_date_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "status",
        "premium_tier",
        "next_start_date",
        "premium_tier_min_premium",
        "distinct_holder_id_in_premium_tier",
        F.round(F.avg("premium").over(w_premium_tier_status), 2).alias("premium_tier_status_avg_premium"),
        F.count(F.lit(1)).over(w_premium_tier_status).alias("rows_in_premium_tier_status"),
        F.last("end_date", ignorenulls=True).over(w_holder_id_by_distinct_holder).alias("last_known_end_date"),
        F.sum("end_date_rank").over(w_holder_id_by_distinct_holder_2).alias("end_date_rank_sum_6_prior"),
        F.row_number().over(w_holder_id_by_distinct_holder_3).alias("rn"),
    )
)

latest = (
    metrics
    .withColumn("rn_2", F.row_number().over(w_latest))
    .filter(F.col("rn_2") == 1)
    .drop("rn_2")
)

result = (
    latest
    .groupBy("last_known_end_date")
    .pivot("premium_tier", ["MEDIUM", "HIGH", "LOW"])
    .agg(F.max("end_date_rank_sum_6_prior"))
    .orderBy("last_known_end_date")
)
