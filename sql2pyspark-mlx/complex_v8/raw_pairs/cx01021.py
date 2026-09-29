from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_start_date_year_mo = Window.partitionBy("holder_id", F.date_trunc("month", "start_date_year"))
w_status_status_group_by_start = Window.partitionBy("status", "status_group").orderBy(F.col("start_date_year").desc(), "id").rowsBetween(-2, -1)
w_holder_id_by_end_date_runnin = Window.partitionBy("holder_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product = Window.partitionBy("product")
w_status_status_group_by_start_2 = Window.partitionBy("status", "status_group").orderBy(F.col("start_date_year").desc(), "id")
w_holder_id_by_end_date = Window.partitionBy("holder_id").orderBy("end_date", "id")
w_holder_id_status_group_by_st = Window.partitionBy("holder_id", "status_group").orderBy("start_date_year", "id")

recent_policys = (
    df_policies
    .filter(F.col("status") == "CANCELLED")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.date_trunc("year", F.col("start_date")).alias("start_date_year"),
        F.when(F.col("status").isin("ACTIVE", "LAPSED"), "group_a").when(F.col("status") == "EXPIRED", "group_b").otherwise("other").alias("status_group"),
    )
)

metrics = (
    recent_policys
    .select(
        "*",
        F.size(F.collect_set("product").over(w_holder_id_start_date_year_mo)).alias("distinct_product_in_holder_id_start_date_year_mo"),
        F.sum("premium").over(w_status_status_group_by_start).alias("premium_sum_2_prior"),
        F.min("premium").over(w_holder_id_by_end_date_runnin).alias("min_premium_so_far"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product), F.lit(0)), 4).alias("premium_share_of_product"),
        F.round(F.percent_rank().over(w_status_status_group_by_start_2), 3).alias("start_date_year_percent_rank"),
        F.lead("premium").over(w_holder_id_by_end_date).alias("next_premium"),
        F.count(F.lit(1)).over(w_product).alias("rows_in_product"),
        F.lead("start_date_year", 2).over(w_status_status_group_by_start_2).alias("next2_start_date_year"),
        F.min("premium").over(w_holder_id_start_date_year_mo).alias("holder_id_start_date_year_month_min_premium"),
        F.ntile(3).over(w_holder_id_status_group_by_st).alias("start_date_year_tercile"),
    )
)

labeled = (
    metrics
    .select(
        "*",
        F.add_months(F.col("end_date"), 6).alias("end_date_plus_6m"),
        F.abs(F.col("premium") - 2111.3).alias("premium_distance"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
)

result = (
    labeled
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        "status_group",
        "distinct_product_in_holder_id_start_date_year_mo",
        "premium_share_of_product",
        "start_date_year_percent_rank",
        "rows_in_product",
        "next2_start_date_year",
        "start_date_year_tercile",
        "end_date_plus_6m",
        "premium_distance",
        "premium_bucket",
    )
    .orderBy(
        F.col("status_group").desc(),
        F.col("distinct_product_in_holder_id_start_date_year_mo").desc_nulls_first(),
    )
)
