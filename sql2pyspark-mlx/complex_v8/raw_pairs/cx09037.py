from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_product_by_end_dat = Window.partitionBy("holder_id", "product").orderBy(F.col("end_date").desc(), "id")
w_holder_region = Window.partitionBy("holder_region")
w_all_by_holder_risk_score = Window.orderBy("holder_risk_score", "id")
w_holder_region_by_status_len = Window.partitionBy("holder_region").orderBy(F.col("status_len").desc(), "id")
w_holder_region_status_len = Window.partitionBy("holder_region", "status_len")
w_holder_region_by_start_date = Window.partitionBy("holder_region").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_policys = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("status").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.col("start_date"), F.col("end_date")).alias("days_end_date_to_start_date"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

with_parent = (
    prepared_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status_len",
        F.col("p.region").alias("holder_region"),
        "p.holder_name",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

with_trends = (
    with_parent
    .select(
        "*",
        F.lead("start_date").over(w_holder_id_product_by_end_dat).alias("next_start_date"),
        F.max("holder_risk_score").over(w_holder_region).alias("holder_region_max_holder_risk_score"),
        F.lead("holder_risk_score", 1, 0).over(w_all_by_holder_risk_score).alias("next_holder_risk_score"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_all_by_holder_risk_score)).alias("days_since_prev_start_date"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_holder_id_product_by_end_dat)).alias("days_since_prev_start_date_2"),
        F.row_number().over(w_holder_region_by_status_len).alias("status_len_seq"),
        F.size(F.collect_set("product").over(w_holder_region)).alias("distinct_product_in_holder_region"),
        F.min("premium").over(w_holder_region_status_len).alias("holder_region_status_len_min_premium"),
        F.sum("status_len").over(w_holder_region_by_start_date).alias("running_status_len"),
        F.ntile(3).over(w_holder_id_product_by_end_dat).alias("end_date_tercile"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "status_len",
        "holder_region",
        "holder_name",
        "holder_risk_score",
        "next_start_date",
        "holder_region_max_holder_risk_score",
        "days_since_prev_start_date",
        "days_since_prev_start_date_2",
        "status_len_seq",
        "distinct_product_in_holder_region",
        "holder_region_status_len_min_premium",
        "end_date_tercile",
        F.coalesce(F.col("holder_region"), F.lit("UNKNOWN")).alias("holder_region_or_unknown"),
        F.coalesce(F.col("holder_region"), F.lit("UNKNOWN")).alias("holder_region_or_unknown_2"),
    )
    .orderBy(
        F.col("holder_region_max_holder_risk_score").asc(),
        F.col("days_since_prev_start_date").desc(),
    )
)
