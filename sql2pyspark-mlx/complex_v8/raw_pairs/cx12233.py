from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_product = Window.partitionBy("holder_id", "product")
w_product_status_by_premium = Window.partitionBy("product", "status").orderBy("premium", "id")
w_status_by_premium_moving_6 = Window.partitionBy("status").orderBy("premium", "id").rowsBetween(-6, Window.currentRow)
w_product_by_premium = Window.partitionBy("product").orderBy("premium", "id")
w_status_by_premium_running = Window.partitionBy("status").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_by_premium_ties = Window.partitionBy("product").orderBy("premium")
w_holder_region_len_status = Window.partitionBy("holder_region_len", "status")
w_status_holder_region = Window.partitionBy("status", "holder_region")
w_status_premium_tercile_by_ho = Window.partitionBy("status", "premium_tercile").orderBy(F.col("holder_risk_score_max_7").desc(), "id")
w_holder_id_holder_region_by_h = Window.partitionBy("holder_id", "holder_region").orderBy("holder_risk_score_max_7", "id").rowsBetween(-6, Window.currentRow)
w_all_by_premium_percent_rank = Window.orderBy(F.col("premium_percent_rank").desc(), "id").rowsBetween(-2, Window.currentRow)

scoped_policys = (
    df_policies
    .filter(F.col("premium") <= 3532.7)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
)

enriched_holder = (
    scoped_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.status",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.region").alias("holder_region"),
    )
)

sequenced = (
    enriched_holder
    .select(
        "*",
        F.size(F.collect_set("status").over(w_holder_id_product)).alias("distinct_status_in_holder_id_product"),
        F.rank().over(w_product_status_by_premium).alias("premium_rank"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id_product), F.lit(0)), 4).alias("premium_share_of_holder_id_product"),
        F.max("holder_risk_score").over(w_status_by_premium_moving_6).alias("holder_risk_score_max_7"),
        F.round(F.col("holder_risk_score") - F.avg("holder_risk_score").over(w_holder_id_product), 2).alias("holder_risk_score_vs_holder_id_product_avg"),
        F.size(F.collect_set("holder_region").over(w_holder_id_product)).alias("distinct_holder_region_in_holder_id_product"),
        F.count(F.lit(1)).over(w_holder_id_product).alias("rows_in_holder_id_product"),
        F.rank().over(w_product_by_premium).alias("premium_rank_2"),
        F.ntile(3).over(w_product_status_by_premium).alias("premium_tercile"),
        F.min("premium").over(w_holder_id_product).alias("holder_id_product_min_premium"),
        F.sum("premium").over(w_status_by_premium_running).alias("running_premium"),
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_holder_id_product), F.lit(0)), 4).alias("holder_risk_score_share_of_holder_id_product"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id_product), 2).alias("premium_vs_holder_id_product_avg"),
        F.max("premium").over(w_holder_id_product).alias("holder_id_product_max_premium"),
        F.round(F.percent_rank().over(w_product_by_premium_ties), 3).alias("premium_percent_rank"),
    )
)

annotated = (
    sequenced
    .select(
        "id",
        "holder_id",
        "product",
        "status",
        "holder_risk_score",
        "holder_region",
        "premium_rank",
        "holder_risk_score_max_7",
        "holder_risk_score_vs_holder_id_product_avg",
        "rows_in_holder_id_product",
        "premium_tercile",
        "holder_id_product_min_premium",
        "running_premium",
        "holder_risk_score_share_of_holder_id_product",
        "premium_percent_rank",
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
        F.length(F.trim(F.col("holder_region"))).alias("holder_region_len"),
        F.lower(F.regexp_replace(F.trim(F.col("holder_region")), " ", "_")).alias("holder_region_slug"),
    )
)

with_windows = (
    annotated
    .select(
        "*",
        F.max("holder_risk_score_max_7").over(w_holder_region_len_status).alias("holder_region_len_status_max_holder_risk_score_m"),
        F.size(F.collect_set("holder_id").over(w_status_holder_region)).alias("distinct_holder_id_in_status_holder_region"),
        F.min("holder_risk_score").over(w_holder_region_len_status).alias("holder_region_len_status_min_holder_risk_score"),
        F.lead("premium_tercile", 1, 0).over(w_status_premium_tercile_by_ho).alias("next_premium_tercile"),
        F.round(F.avg("rows_in_holder_id_product").over(w_holder_id_holder_region_by_h), 2).alias("rows_in_holder_id_product_avg_7"),
        F.max("premium_percent_rank").over(w_all_by_premium_percent_rank).alias("premium_percent_rank_max_3"),
    )
)

result = (
    with_windows
    .filter(F.col("holder_risk_score_share_of_holder_id_product").between(0.1, 0.8))
    .select(
        "id",
        "holder_id",
        "product",
        "status",
        "holder_risk_score",
        "holder_region",
        "premium_rank",
        "holder_risk_score_max_7",
        "rows_in_holder_id_product",
        "premium_tercile",
        "holder_id_product_min_premium",
        "running_premium",
        "holder_risk_score_share_of_holder_id_product",
        "premium_percent_rank",
        "product_slug",
        "holder_region_len",
        "holder_region_len_status_max_holder_risk_score_m",
        "distinct_holder_id_in_status_holder_region",
        "holder_region_len_status_min_holder_risk_score",
        "next_premium_tercile",
        "rows_in_holder_id_product_avg_7",
    )
    .orderBy(F.col("premium_tercile").desc())
)
