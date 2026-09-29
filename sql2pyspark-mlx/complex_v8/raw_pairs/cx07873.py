from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_status = Window.partitionBy("holder_id", "status")

filtered = (
    df_policies
    .filter(F.col("start_date").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.concat(F.col("status"), F.lit("#"), F.col("holder_id").cast("string")).alias("status_ref"),
        F.abs(F.col("premium") - 3465.4).alias("premium_distance"),
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
    )
)

with_trends = (
    filtered
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        "status_ref",
        "premium_distance",
        F.sum("premium").over(w_holder_id_status).alias("holder_id_status_sum_premium"),
        F.size(F.collect_set("product").over(w_holder_id_status)).alias("distinct_product_in_holder_id_status"),
    )
)

with_parent = (
    with_trends.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.end_date",
        "s.status",
        "s.holder_id_status_sum_premium",
        "s.distinct_product_in_holder_id_status",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "holder_id",
        "premium",
        "status",
        "holder_id_status_sum_premium",
        "holder_segment",
    )
    .orderBy(F.col("status").desc())
)
