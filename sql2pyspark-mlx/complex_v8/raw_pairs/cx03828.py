from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_status = Window.partitionBy("holder_id", "status")
w_end_date_year = Window.partitionBy(F.year("end_date"))
w_end_date_month_holder_id_by = Window.partitionBy(F.date_trunc("month", "end_date"), "holder_id").orderBy(F.col("end_date").asc_nulls_last(), "id")
w_end_date_month_holder_id_by_2 = Window.partitionBy(F.date_trunc("month", "end_date"), "holder_id").orderBy(F.col("end_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_by_premium_running = Window.partitionBy("holder_id").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_status_by_end_date = Window.partitionBy("holder_id", "status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_premium_moving_6 = Window.partitionBy("status").orderBy(F.col("premium").desc(), "id").rowsBetween(-6, Window.currentRow)
w_holder_id_by_premium_ties = Window.partitionBy("holder_id").orderBy(F.col("premium").desc())
w_holder_id_status_by_end_date_2 = Window.partitionBy("holder_id", "status").orderBy("end_date", "id").rowsBetween(-3, -1)
w_holder_id_by_premium = Window.partitionBy("holder_id").orderBy(F.col("premium").desc(), "id")

recent = (
    df_policies
    .filter(F.col("product") == "AUTO")
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
        F.concat(F.col("product"), F.lit("#"), F.col("id").cast("string")).alias("product_ref"),
    )
)

with_trends = (
    recent
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
        F.round(F.avg("premium").over(w_holder_id_status), 2).alias("holder_id_status_avg_premium"),
        F.max("premium").over(w_end_date_year).alias("end_date_year_max_premium"),
        F.max("premium").over(w_holder_id_status).alias("holder_id_status_max_premium"),
        F.ntile(4).over(w_end_date_month_holder_id_by).alias("end_date_quartile"),
        F.round(F.avg("premium").over(w_end_date_year), 2).alias("end_date_year_avg_premium"),
        F.first("end_date").over(w_end_date_month_holder_id_by_2).alias("first_end_date"),
        F.sum("premium").over(w_holder_id_by_premium_running).alias("running_premium"),
        F.nth_value("end_date", 2).over(w_holder_id_status_by_end_date).alias("second_end_date"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_end_date_year), F.lit(0)), 4).alias("premium_share_of_end_date_year"),
        F.round(F.avg("premium").over(w_status_by_premium_moving_6), 2).alias("premium_avg_7"),
        F.round(F.percent_rank().over(w_holder_id_by_premium_ties), 3).alias("premium_percent_rank"),
        F.size(F.collect_set("status").over(w_end_date_year)).alias("distinct_status_in_end_date_year"),
        F.round(F.avg("premium").over(w_holder_id_status_by_end_date_2), 2).alias("premium_avg_3_prior"),
        F.round(F.cume_dist().over(w_holder_id_by_premium), 3).alias("premium_cume_dist"),
        F.size(F.collect_set("holder_id").over(w_end_date_year)).alias("distinct_holder_id_in_end_date_year"),
    )
)

labeled = (
    with_trends
    .filter(F.col("status").isin("LAPSED", "ACTIVE"))
    .select(
        "*",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.when(F.col("status").isin("EXPIRED", "LAPSED"), "group_a").when(F.col("status") == "ACTIVE", "group_b").otherwise("other").alias("status_group"),
    )
)

result = (
    labeled
    .select(
        "id",
        "holder_id",
        "end_date",
        "status",
        "holder_id_status_avg_premium",
        "holder_id_status_max_premium",
        "end_date_quartile",
        "end_date_year_avg_premium",
        "second_end_date",
        "premium_share_of_end_date_year",
        "premium_avg_7",
        "distinct_status_in_end_date_year",
        "premium_cume_dist",
        "distinct_holder_id_in_end_date_year",
        "status_group",
    )
    .orderBy(
        F.col("holder_id_status_max_premium").desc(),
        F.col("status_group").desc(),
    )
)
